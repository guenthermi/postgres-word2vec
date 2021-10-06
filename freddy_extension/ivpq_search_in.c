#define OPT_PREFETCH
#define OPT_FAST_PV_TOPK_UPDATE
#define USE_MULTI_COARSE

// clang-format off

#include "postgres.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "time.h"
#include "stdio.h"

#include "catalog/pg_type.h"

#include "index_utils.h"
#include "output_utils.h"

// clang-format on

void initTargetLists(TargetListElem **result, int queryIndicesSize,
                            const int target_lists_size, const int method) {
  TargetListElem *targetLists =
      palloc(queryIndicesSize * sizeof(struct TargetListElem));
  for (int i = 0; i < queryIndicesSize; i++) {
    if (method != EXACT_CALC) {
      targetLists[i].codes = palloc(sizeof(int16 *) * target_lists_size);
    }
    targetLists[i].ids = palloc(sizeof(int) * target_lists_size);
    targetLists[i].size = 0;
    targetLists[i].next = NULL;
    targetLists[i].last = &targetLists[i];
    if ((method == PQ_PV_CALC) || (method == EXACT_CALC)) {
      targetLists[i].vectors = palloc(sizeof(float4 *) * target_lists_size);
    }
  }
  *result = targetLists;
}

void reorderTopKPV(TopKPV tk, int k, int *fillLevel, float *maxDist) {
  qsort(tk, *fillLevel, sizeof(TopKPVEntry), cmpTopKPVEntry);
  *fillLevel = k;
  *maxDist = tk[k - 1].distance;
}

void updateTopKPVFast(TopKPV tk, const int batchSize, int k,
                             int *fillLevel, float *maxDist, int dim, int id,
                             float distance, float4 *vector) {
  tk[*fillLevel].id = id;
  tk[*fillLevel].distance = distance;
  tk[*fillLevel].vector = vector;

  (*fillLevel)++;
  if (*fillLevel == (batchSize - 1)) {
    reorderTopKPV(tk, k, fillLevel, maxDist);
  }
}

PG_FUNCTION_INFO_V1(ivpq_search_in);

Datum ivpq_search_in(PG_FUNCTION_ARGS) {
  const float MAX_DIST = 1000.0;
  const int TOPK_BATCH_SIZE = 200;
  const int TARGET_LISTS_SIZE = 100;

  FuncCallContext *funcctx;
  TupleDesc outtertupdesc;
  AttInMetadata *attinmeta;
  UsrFctxBatch *usrfctx;

  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;

    // input parameter
    float4 **queryVectors;
    int queryVectorsSize;
    int k;
    int *inputIds;
    int inputIdsSize;
    int *queryIds;

    int alpha_original;  // size of search space is set to about
                         // SE*inputTermsSize
                         // vectors
    int alpha;
    int pvf;     // post verification factor
    int method;  // PQ / EXACT
    bool useTargetLists;
    float confidence;

    // search parameters
    int queryDim;
    int subvectorSize = 0;

    CodebookCompound cb;
    int double_threshold = 0;
    bool double_codes = false;
    int codesNumber = 0;

#ifdef USE_MULTI_COARSE
    CodebookCompound cqMulti;
    char *tableNameCQ = palloc(sizeof(char) * 100);
#endif
#ifndef USE_MULTI_COARSE
    CoarseQuantizer cq;
#endif

    int cqSize;

    float *statistics;

    // output variables
    TopK *topKs;
    float *maxDists;

    // time measurement
    clock_t start = 0;
    clock_t end = 0;
    clock_t last = 0;
    clock_t sub_start = 0;
    clock_t sub_end = 0;

    // helper variables
    int n = 0;
    Datum *queryIdData;

    int queryVectorsIndicesSize;
    int *queryVectorsIndices;

    TopKPV *topKPVs;
    int *fillLevels = NULL;

    TargetLists targetLists = NULL;
    int *targetCounts = NULL;  // to determine if enough targets are observed

    // for coarse quantizer
    int **cqIdsMulti;
    int **cqTableIds;
    int *cqTableIdCounts;

    // for pq similarity calculation
    float4 **querySimilarities = NULL;

    Datum *idsData;
    Datum *i_data;  // for query vectors

    ResultInfo rInfo;

    char *command;
    char *cur;

    char *tableName = palloc(sizeof(char) * 100);
    char *tableNameCodebook = palloc(sizeof(char) * 100);
    char *tableNameFineQuantizationIVPQ = palloc(sizeof(char) * 100);

    elog(INFO, "start query");
    start = clock();
    last = clock();

    getTableName(NORMALIZED, tableName, 100);
    getTableName(IVPQ_CODEBOOK, tableNameCodebook, 100);
    getTableName(IVPQ_QUANTIZATION, tableNameFineQuantizationIVPQ, 100);

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    // get input parameter
    getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data, &n);
    queryVectors = palloc(n * sizeof(float4 *));
    queryVectorsSize = n;
    for (int i = 0; i < n; i++) {
      queryDim = 0;
      convert_bytea_float4(DatumGetByteaP(i_data[i]), &queryVectors[i],
                           &queryDim);
    }
    n = 0;
    // for the output it is necessary to map query vectors to ids
    getArray(PG_GETARG_ARRAYTYPE_P(1), &queryIdData, &n);
    if (n != queryVectorsSize) {
      elog(ERROR, "Number of query vectors and query vector ids differs! ( %d, %d)", n, queryVectorsSize);
    }
    queryIds = palloc(queryVectorsSize * sizeof(int));
    for (int i = 0; i < queryVectorsSize; i++) {
      queryIds[i] = DatumGetInt32(queryIdData[i]);
    }
    n = 0;

    k = PG_GETARG_INT32(2);
    getArray(PG_GETARG_ARRAYTYPE_P(3), &idsData, &n);  // target words
    inputIds = palloc(n * sizeof(int));

    for (int j = 0; j < n; j++) {
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdsSize = n;

    // parameter inputs
    alpha_original = PG_GETARG_INT32(4);
    pvf = PG_GETARG_INT32(5);
    method = PG_GETARG_INT32(
        6);  // (0: PQ / 1: EXACT / 2: PQ with post verification)
    useTargetLists = PG_GETARG_BOOL(7);
    confidence = PG_GETARG_FLOAT4(8);
    double_threshold = PG_GETARG_INT32(9);
    alpha = alpha_original;
    if (pvf < 1) {
      pvf = 1;
    }

    queryVectorsIndicesSize = queryVectorsSize;
    queryVectorsIndices = palloc(sizeof(int) * queryVectorsSize);
    for (int i = 0; i < queryVectorsSize; i++) {
      queryVectorsIndices[i] = i;
    }

    if ((method == PQ_CALC) || (method == PQ_PV_CALC)) {
      // get codebook
      cb = getCodebook(tableNameCodebook);
      subvectorSize = queryDim / cb.positions;
    }
// get coarse quantizer
#ifdef USE_MULTI_COARSE
    getTableName(COARSE_QUANTIZATION_MULTI, tableNameCQ, 100);
    cqMulti = getCodebook(tableNameCQ);
    cqSize = pow(cqMulti.codeSize, cqMulti.positions);
#endif
#ifndef USE_MULTI_COARSE
    cq = getCoarseQuantizer(&cqSize);
#endif
    sub_start = clock();
    // get statistics about coarse centroid distribution
    statistics = getStatistics();
    sub_end = clock();
    elog(INFO, "TRACK get_statistics_time %f",
         (double)(sub_end - sub_start) / CLOCKS_PER_SEC);
    elog(INFO, "new iteration: alpha %d", alpha);
    // init topk data structures
    initTopKs(&topKs, &maxDists, queryVectorsSize, k, MAX_DIST);
    targetCounts = palloc(sizeof(int) * queryVectorsSize);
    for (int i = 0; i < queryVectorsSize; i++) {
      targetCounts[i] = 0;
    }
    if (method == PQ_PV_CALC) {
#ifdef OPT_FAST_PV_TOPK_UPDATE
      initTopKPVs(&topKPVs, &maxDists, queryVectorsSize,
                  TOPK_BATCH_SIZE + k * pvf, MAX_DIST, queryDim);
      fillLevels = palloc(queryVectorsSize * sizeof(int));
      for (int i = 0; i < queryVectorsSize; i++) {
        fillLevels[i] = 0;
      }
#endif
#ifndef OPT_FAST_PV_TOPK_UPDATE
      initTopKPVs(&topKPVs, &maxDists, queryVectorsSize, k * pvf, MAX_DIST,
                  queryDim);
#endif
      end = clock();
      elog(INFO, "TRACK setup_topkpv_time %f",
           (double)(end - last) / CLOCKS_PER_SEC);
    }

    if ((method == PQ_CALC) || (method == PQ_PV_CALC)) {
      if (alpha * k > double_threshold) {
        double_codes = true;
      } else {
        double_codes = false;
      }
      // compute querySimilarities (precomputed distances) for product
      // quantization
      if (double_codes) {
        codesNumber = cb.positions / 2;
        querySimilarities = palloc(sizeof(float4 *) * queryVectorsSize);
        for (int i = 0; i < queryVectorsSize; i++) {
          querySimilarities[i] =
              palloc(codesNumber * cb.codeSize * cb.codeSize * sizeof(float4));
          getPrecomputedDistancesDouble(querySimilarities[i], cb.positions,
                                        cb.codeSize, subvectorSize,
                                        queryVectors[i], cb.codebook);
        }

      } else {
        codesNumber = cb.positions;
        querySimilarities = palloc(sizeof(float4 *) * queryVectorsSize);
        for (int i = 0; i < queryVectorsSize; i++) {
          querySimilarities[i] =
              palloc(cb.positions * cb.codeSize * sizeof(float4));
          getPrecomputedDistances(querySimilarities[i], cb.positions,
                                  cb.codeSize, subvectorSize, queryVectors[i],
                                  cb.codebook);
        }
      }
    }

    end = clock();
    elog(INFO, "TRACK precomputation_time %f",
         (double)(end - last) / CLOCKS_PER_SEC);
    last = clock();

    rInfo.proc = 0;
    while (queryVectorsIndicesSize > 0) {
      // compute coarse ids
      int coarse_ids_size = 0;
      int *blacklist =
          palloc(sizeof(int) *
                 cqSize);  // query should not contain coarse ids multiple times
      int *coarse_ids = palloc(sizeof(int) * cqSize);

      int newQueryVectorsIndicesSize = 0;
      int *newQueryVectorsIndices = NULL;
      bool lastIteration;

      if (useTargetLists) {
        sub_start = clock();
        initTargetLists(&targetLists, queryVectorsSize, TARGET_LISTS_SIZE,
                        method);
        sub_end = clock();
        elog(INFO, "TRACK init_targetlist_time %f",
             (double)(sub_end - sub_start) / CLOCKS_PER_SEC);
      }

      for (int i = 0; i < cqSize;
           i++) {  // for construction of target query needed
        blacklist[i] = 0;
      }

      sub_start = clock();
#ifdef USE_MULTI_COARSE
      lastIteration = determineCoarseIdsMultiWithStatisticsMulti(
          &cqIdsMulti, &cqTableIds, &cqTableIdCounts, queryVectorsIndices,
          queryVectorsIndicesSize, queryVectorsSize, MAX_DIST, cqMulti.codebook,
          cqSize, cqMulti.positions, cqMulti.codeSize, queryVectors, queryDim,
          statistics, inputIdsSize, (k * alpha), confidence);
#endif
#ifndef USE_MULTI_COARSE
      lastIteration = determineCoarseIdsMultiWithStatistics(
          &cqIdsMulti, &cqTableIds, &cqTableIdCounts, queryVectorsIndices,
          queryVectorsIndicesSize, queryVectorsSize, MAX_DIST, cq, cqSize,
          queryVectors, queryDim, statistics, inputIdsSize, (k * alpha),
          confidence);
#endif
      sub_end = clock();
      elog(INFO, "TRACK determine_coarse_quantization_time %f",
           (double)(sub_end - sub_start) / CLOCKS_PER_SEC);

      for (int i = 0; i < cqSize; i++) {
        if (cqTableIdCounts[i] > 0) {
          coarse_ids[coarse_ids_size] = i;
          coarse_ids_size += 1;
        }
      }

      end = clock();
      command = palloc(inputIdsSize * 100 * sizeof(char) +
                       20 * coarse_ids_size * sizeof(char) + 500);
      cur = command;
      switch (method) {
        case PQ_CALC:
          cur += sprintf(
              cur, "SELECT fq.id, vector, fq.coarse_id FROM %s AS fq WHERE (",
              tableNameFineQuantizationIVPQ);
          break;
        case PQ_PV_CALC:
          cur += sprintf(cur,
                         "SELECT fq.id, fq.vector, vecs.vector, fq.coarse_id "
                         "FROM %s AS fq INNER JOIN %s AS vecs ON fq.id "
                         "= vecs.id WHERE (",
                         tableNameFineQuantizationIVPQ, tableName);
          break;
        case EXACT_CALC:
          cur += sprintf(cur,
                         "SELECT fq.id, vecs.vector, fq.coarse_id FROM %s AS "
                         "fq INNER JOIN %s AS vecs ON fq.id = vecs.id "
                         "WHERE (",
                         tableNameFineQuantizationIVPQ, tableName);
          break;
        default:
          elog(ERROR, "Unknown computation method!");
      }
      // fill command
      cur += sprintf(cur, "(coarse_id IN ( ");
      for (int i = 0; i < coarse_ids_size; i++) {
        if (i != 0) {
          cur += sprintf(cur, ",%d", coarse_ids[i]);
        } else {
          cur += sprintf(cur, "%d", coarse_ids[i]);
        }
      }
      cur += sprintf(cur, "))) AND (fq.id IN (");
      for (int i = 0; i < inputIdsSize; i++) {
        if (i == inputIdsSize - 1) {
          cur += sprintf(cur, "%d", inputIds[i]);
        } else {
          cur += sprintf(cur, "%d,", inputIds[i]);
        }
      }
      sprintf(cur, "))");
      end = clock();
      elog(INFO, "TRACK query_construction_time %f",
           (double)(end - last) / CLOCKS_PER_SEC);
      last = clock();
      SPI_connect();
      rInfo.ret = SPI_execute(command, true, 0);
      end = clock();
      elog(INFO, "TRACK data_retrieval_time %f",
           (double)(end - last) / CLOCKS_PER_SEC);
      last = clock();
      rInfo.proc = SPI_processed;
      elog(INFO, "TRACK retrieved %d results", rInfo.proc);
      if (rInfo.ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        int i;
        long counter = 0;
        float4 *vector;  // for post verification
        int offset =
            (method == PQ_PV_CALC) ? 4 : 3;  // position offset for coarseIds
        int l;
        int16 *codes2;
        int codeRange = double_codes ? cb.codeSize * cb.codeSize : cb.codeSize;
        for (i = 0; i < rInfo.proc; i++) {
          int coarseId;
          int16 *codes;
          int wordId;
          float distance;
          float *next;
          HeapTuple tuple = tuptable->vals[i];
          wordId = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &rInfo.info));
          n = 0;
          if ((method == PQ_PV_CALC) || (method == PQ_CALC)) {
            convert_bytea_int16(
                DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 2, &rInfo.info)),
                &codes, &n);
            n = 0;
          }
          if (method == EXACT_CALC) {
            convert_bytea_float4(
                DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 2, &rInfo.info)),
                &vector, &n);
            n = 0;
          }
          if (method == PQ_PV_CALC) {
            convert_bytea_float4(
                DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 3, &rInfo.info)),
                &vector, &n);
            n = 0;
          }
          if (double_codes) {
            codes2 = palloc(sizeof(int) * codesNumber);
            for (l = 0; l < codesNumber; l++) {
              codes2[l] = codes[l * 2] + codes[l * 2 + 1] * cb.codeSize;
            }
          } else {
            codes2 = codes;
          }

          // read coarse ids
          coarseId =
              DatumGetInt32(SPI_getbinval(tuple, tupdesc, offset, &rInfo.info));
          // calculate distances
          for (int j = 0; j < cqTableIdCounts[coarseId]; j++) {
            int queryVectorsIndex = cqTableIds[coarseId][j];
            targetCounts[queryVectorsIndex] += 1;
            if (useTargetLists) {
#ifdef OPT_PREFETCH
              if (j % 25 == 0) {
                for (int n = 0; n < 50; n++) {
                  if (j + n < cqTableIdCounts[coarseId]) {
                    __builtin_prefetch(&cqTableIds[coarseId][j + n], 0);
                    __builtin_prefetch(
                        &targetLists[cqTableIds[coarseId][j + n]], 0);
                    __builtin_prefetch(
                        &(*(targetLists[cqTableIds[coarseId][j + n]]).last), 1);
                  }
                }
              }
#endif /*OPT_PREFETCH*/
              // add codes and word id to the target list which corresonds to
              // the query
              addToTargetList(targetLists, queryVectorsIndex, TARGET_LISTS_SIZE,
                              method, codes2, vector, wordId);
            } else {
              if ((method == PQ_CALC) || (method == PQ_PV_CALC)) {
#ifdef OPT_PREFETCH
                if (j % 100 == 0) {
                  for (int n = 0; n < 200; n++) {
                    if (j + n < cqTableIdCounts[coarseId]) {
                      next = querySimilarities[cqTableIds[coarseId][j + n]];
                      for (l = 0; l < cb.positions; l++) {
                        __builtin_prefetch(&next[cb.codeSize * l + codes2[l]],
                                           0);
                      }
                    }
                  }
                }
#endif /*OPT_PREFETCH*/

                if (double_codes) {
                  distance = computePQDistanceInt16(
                      querySimilarities[queryVectorsIndex], codes2, codesNumber,
                      codeRange);
                } else {
                  distance = computePQDistanceInt16(
                      querySimilarities[queryVectorsIndex], codes2, codesNumber,
                      codeRange);
                }
                if (method == PQ_PV_CALC) {
                  if (distance < maxDists[queryVectorsIndex]) {
#ifdef OPT_FAST_PV_TOPK_UPDATE
                    updateTopKPVFast(topKPVs[queryVectorsIndex],
                                     TOPK_BATCH_SIZE + k * pvf, k * pvf,
                                     &fillLevels[queryVectorsIndex],
                                     &maxDists[queryVectorsIndex], queryDim,
                                     wordId, distance, vector);
#endif
#ifndef OPT_FAST_PV_TOPK_UPDATE
                    updateTopKPV(topKPVs[queryVectorsIndex], distance, wordId,
                                 k * pvf, maxDists[queryVectorsIndex], vector,
                                 queryDim);
                    maxDists[queryVectorsIndex] =
                        topKPVs[queryVectorsIndex][k * pvf - 1].distance;
#endif
                  }
                }
                if (method == PQ_CALC) {
                  if (distance < maxDists[queryVectorsIndex]) {
                    updateTopK(topKs[queryVectorsIndex], distance, wordId, k,
                               maxDists[queryVectorsIndex]);
                    maxDists[queryVectorsIndex] =
                        topKs[queryVectorsIndex][k - 1].distance;
                  }
                }
              } else {
                // method == EXACT_CALC
                distance = squareDistance(queryVectors[queryVectorsIndex],
                                          vector, queryDim);
                if (distance < maxDists[queryVectorsIndex]) {
                  updateTopK(topKs[queryVectorsIndex], distance, wordId, k,
                             maxDists[queryVectorsIndex]);
                  maxDists[queryVectorsIndex] =
                      topKs[queryVectorsIndex][k - 1].distance;
                }
              }
            }
          }
        }

        if (useTargetLists) {
          elog(INFO, "TRACK reorder_time %f",
               (double)(clock() - last) / CLOCKS_PER_SEC);
          // calculate distances with target list
          for (i = 0; i < queryVectorsIndicesSize; i++) {
            int queryVectorsIndex = queryVectorsIndices[i];
            TargetListElem *current = &targetLists[queryVectorsIndex];
            if ((targetCounts[queryVectorsIndex] < k * alpha_original) &&
                !lastIteration) {
              targetCounts[queryVectorsIndex] = 0;
              continue;
            }
            while (current != NULL) {
              float distance;
              for (int j = 0; j < current->size; j++) {
                if ((method == PQ_CALC) || (method == PQ_PV_CALC)) {
                  distance = computePQDistanceInt16(
                      querySimilarities[queryVectorsIndex], current->codes[j],
                      codesNumber, codeRange);
                  if (method == PQ_PV_CALC) {
                    if (distance < maxDists[queryVectorsIndex]) {
#ifdef OPT_FAST_PV_TOPK_UPDATE
                      updateTopKPVFast(
                          topKPVs[queryVectorsIndex], TOPK_BATCH_SIZE + k * pvf,
                          k * pvf, &fillLevels[queryVectorsIndex],
                          &maxDists[queryVectorsIndex], queryDim,
                          current->ids[j], distance, current->vectors[j]);
#endif
#ifndef OPT_FAST_PV_TOPK_UPDATE
                      updateTopKPV(topKPVs[queryVectorsIndex], distance,
                                   current->ids[j], k * pvf,
                                   maxDists[queryVectorsIndex],
                                   current->vectors[j], queryDim);
                      maxDists[queryVectorsIndex] =
                          topKPVs[queryVectorsIndex][k * pvf - 1].distance;
#endif
                    }
                  }
                  if (method == PQ_CALC) {
                    if (distance < maxDists[queryVectorsIndex]) {
                      updateTopK(topKs[queryVectorsIndex], distance,
                                 current->ids[j], k,
                                 maxDists[queryVectorsIndex]);
                      maxDists[queryVectorsIndex] =
                          topKs[queryVectorsIndex][k - 1].distance;
                    }
                  }
                } else {
                  // method == EXACT_CALC
                  distance = squareDistance(queryVectors[queryVectorsIndex],
                                            current->vectors[j], queryDim);
                  if (distance < maxDists[queryVectorsIndex]) {
                    updateTopK(topKs[queryVectorsIndex], distance,
                               current->ids[j], k, maxDists[queryVectorsIndex]);
                    maxDists[queryVectorsIndex] =
                        topKs[queryVectorsIndex][k - 1].distance;
                  }
                }
              }
              current = current->next;
            }
          }
        }

        // post verification
        if (method == PQ_PV_CALC) {
#ifdef OPT_FAST_PV_TOPK_UPDATE
          sub_start = clock();
          for (int i = 0; i < queryVectorsIndicesSize; i++) {
            int queryIndex = queryVectorsIndices[i];
            reorderTopKPV(topKPVs[queryIndex], k * pvf, &fillLevels[queryIndex],
                          &maxDists[queryIndex]);
          }
          sub_end = clock();
          elog(INFO, "TRACK topkpv_reorder_time %f",
               (double)(sub_end - sub_start) / CLOCKS_PER_SEC);
#endif
          sub_start = clock();
          postverify(queryVectorsIndices, queryVectorsIndicesSize, k, pvf,
                     topKPVs, topKs, queryVectors, queryDim, MAX_DIST);
          sub_end = clock();
          elog(INFO, "TRACK pv_computation_time %f",
               (double)(sub_end - sub_start) / CLOCKS_PER_SEC);
        }

        end = clock();
        elog(INFO, "TRACK computation_time %f",
             (double)(end - last) / CLOCKS_PER_SEC);
        last = clock();
        elog(INFO, "finished computation counter %ld  ", counter);
      }
      SPI_finish();
      // recalcalculate queryIndices
      if (!lastIteration) {
        newQueryVectorsIndicesSize = 0;
        newQueryVectorsIndices = palloc(sizeof(int) * queryVectorsIndicesSize);
        for (int i = 0; i < queryVectorsIndicesSize; i++) {
          if (topKs[queryVectorsIndices[i]][k - 1].distance == MAX_DIST) {
            newQueryVectorsIndices[newQueryVectorsIndicesSize] =
                queryVectorsIndices[i];
            newQueryVectorsIndicesSize++;
            // empty topk
            initTopK(&topKs[queryVectorsIndices[i]], k, MAX_DIST);
            maxDists[queryVectorsIndices[i]] = MAX_DIST;
            if (method == PQ_PV_CALC) {
#ifdef OPT_FAST_PV_TOPK_UPDATE
              initTopKPV(&topKPVs[queryVectorsIndices[i]],
                         TOPK_BATCH_SIZE + k * pvf, MAX_DIST, queryDim);
              fillLevels[i] = 0;
#endif
#ifndef OPT_FAST_PV_TOPK_UPDATE
              initTopKPV(&topKPVs[queryVectorsIndices[i]], k * pvf, MAX_DIST,
                         queryDim);
#endif
            }
          }
        }
        elog(INFO, "newQueryVectorsIndicesSize: %d",
             newQueryVectorsIndicesSize);
        queryVectorsIndicesSize = newQueryVectorsIndicesSize;
        queryVectorsIndices = newQueryVectorsIndices;
      } else {
        queryVectorsIndicesSize = 0;
      }
      end = clock();
      elog(INFO, "TRACK recalculate_query_indices_time %f",
           (double)(end - last) / CLOCKS_PER_SEC);
      last = clock();
      elog(INFO, "newQueryVectorsIndicesSize %d", newQueryVectorsIndicesSize);

      // claculate new se
      if (lastIteration) {
        queryVectorsIndicesSize = 0;
      }
      alpha += alpha;

      elog(INFO, "se: %d queryVectorsIndicesSize: %d", alpha,
           queryVectorsIndicesSize);
    }
    // return tokKs
    usrfctx = (UsrFctxBatch *)palloc(sizeof(UsrFctxBatch));
    fillUsrFctxBatch(usrfctx, queryIds, queryVectorsSize, topKs, k);
    funcctx->user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc(3);

    TupleDescInitEntry(outtertupdesc, 1, "QueryId", INT4OID, -1, 0);
    TupleDescInitEntry(outtertupdesc, 2, "TargetId", INT4OID, -1, 0);
    TupleDescInitEntry(outtertupdesc, 3, "Distance", FLOAT4OID, -1, 0);
    attinmeta = TupleDescGetAttInMetadata(outtertupdesc);
    funcctx->attinmeta = attinmeta;
    end = clock();
    elog(INFO, "TRACK total_time %f", (double)(end - start) / CLOCKS_PER_SEC);
    MemoryContextSwitchTo(oldcontext);
  }
  funcctx = SRF_PERCALL_SETUP();
  usrfctx = (UsrFctxBatch *)funcctx->user_fctx;
  // return results
  if (usrfctx->iter >= usrfctx->k * usrfctx->queryIdsSize) {
    SRF_RETURN_DONE(funcctx);
  } else {
    Datum result;
    HeapTuple outTuple;
    snprintf(usrfctx->values[0], 16, "%d",
             usrfctx->queryIds[usrfctx->iter / usrfctx->k]);
    snprintf(
        usrfctx->values[1], 16, "%d",
        usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].id);
    snprintf(usrfctx->values[2], 16, "%f",
             usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k]
                 .distance);
    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings(funcctx->attinmeta, usrfctx->values);
    result = HeapTupleGetDatum(outTuple);
    SRF_RETURN_NEXT(funcctx, result);
  }
}
