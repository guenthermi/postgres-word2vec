#define OPT_PREFETCH
#define OPT_FAST_PV_TOPK_UPDATE
#define USE_MULTI_COARSE

#include "postgres.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "stdio.h"
#include "stdlib.h"
#include "time.h"

#include "hashmap.h"

#include "catalog/pg_type.h"

#include "index_utils.h"
#include "output_utils.h"

#include "assert.h"

inline void getPrecomputedDistances(float4* preDists, int cbPositions, int cbCodes, int subvectorSize, float4* queryVector, Codebook cb){
  for (int i=0; i< cbPositions*cbCodes; i++){
      int pos = cb[i].pos;
      int code = cb[i].code;
      float* vector = cb[i].vector;
      preDists[pos*cbCodes + code] = squareDistance(queryVector+(pos*subvectorSize), vector, subvectorSize);
  }
}

inline void getPrecomputedDistancesDouble(float4* preDists, int cbPositions, int cbCodes, int subvectorSize, float4* queryVector, Codebook cb){
  for (int i=0; i < (cbPositions/2); i++){
    int pos = i*2;
    int pointer = cbCodes*cbCodes*i;

    for (int j=0; j< cbCodes*cbCodes; j++){
      int p1 = (j % cbCodes)+pos*cbCodes; // positions in cb
      int p2 = (j / cbCodes)+(pos+1)*cbCodes;
      int code = cb[p1].code + cbCodes*cb[p2].code;
      preDists[pointer + code] = squareDistance(queryVector+(pos*subvectorSize), cb[p1].vector, subvectorSize) +
        squareDistance(queryVector+((pos+1)*subvectorSize), cb[p2].vector, subvectorSize);
    }
  }
}


inline float computePQDistance(float* preDists, int* codes, int cbPositions, int cbCodes){
  float distance = 0;
  for (int l = 0; l < cbPositions; l++){
    distance +=  preDists[cbCodes*l+ codes[l]];
  }
  return distance;
}

inline float computePQDistanceNew(float* preDists, int16* codes, int cbPositions, int cbCodes){
  float distance = 0;
  for (int l = 0; l < cbPositions; l++){
    distance +=  preDists[cbCodes*l+ codes[l]];
  }
  return distance;
}

inline void addToTargetList(TargetListElem* targetLists, int queryVectorsIndex, const int target_lists_size, const int method, int16* codes, float4* vector, int wordId){
  TargetListElem* currentTargetList = targetLists[queryVectorsIndex].last;
  currentTargetList->codes[currentTargetList->size] = codes;
  currentTargetList->ids[currentTargetList->size] = wordId;
  if (method == PQ_PV_CALC){
    currentTargetList->vectors[currentTargetList->size] = vector;
  }
  currentTargetList->size += 1;
  if (currentTargetList->size == target_lists_size){
    currentTargetList->next = palloc(sizeof(TargetListElem));
    currentTargetList->next->codes = palloc(sizeof(int16*)*target_lists_size);
    currentTargetList->next->ids = palloc(sizeof(int)*target_lists_size);
    if (method == PQ_PV_CALC){
      currentTargetList->next->vectors = palloc(sizeof(float4*)*target_lists_size);
    }
    currentTargetList->next->size = 0;
    currentTargetList->next->next = NULL;
    targetLists[queryVectorsIndex].last = currentTargetList->next;
  }
}

inline void initTargetLists(TargetListElem** result, int queryIndicesSize, const int target_lists_size, const int method){
  TargetListElem* targetLists = palloc(queryIndicesSize*sizeof(struct TargetListElem));
  for (int i = 0; i < queryIndicesSize; i++){
    targetLists[i].codes = palloc(sizeof(int16*)*target_lists_size);
    targetLists[i].ids = palloc(sizeof(int)*target_lists_size);
    targetLists[i].size = 0;
    targetLists[i].next = NULL;
    targetLists[i].last = &targetLists[i];
    if (method == PQ_PV_CALC){
      targetLists[i].vectors = palloc(sizeof(float4*)*target_lists_size);
    }
  }
  *result = targetLists;
}

inline void reorderTopKPV(TopKPV tk, int k, int* fillLevel, float* maxDist){
  qsort(tk, *fillLevel, sizeof(TopKPVEntry), cmpTopKPVEntry);
  *fillLevel = k;
  *maxDist = tk[k-1].distance;
}

inline void updateTopKPVFast(TopKPV tk, const int batchSize, int k, int* fillLevel, float* maxDist, int dim, int id, float distance, float4* vector){
  tk[*fillLevel].id = id;
  tk[*fillLevel].distance = distance;
  tk[*fillLevel].vector = vector;
  // memcpy(tk[*fillLevel].vector, vector, dim*sizeof(float4));
  (*fillLevel)++;
  if (*fillLevel == (batchSize-1)){
    reorderTopKPV(tk, k, fillLevel, maxDist);
  }
}

PG_FUNCTION_INFO_V1(pq_search);

Datum
pq_search(PG_FUNCTION_ARGS)
{

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctx *usrfctx;

  if (SRF_IS_FIRSTCALL ())
   {
     clock_t start;
     clock_t end;

     Codebook cb;
     int cbPositions = 0;
     int cbCodes = 0;
     float* queryVector;
     int k;
     int subvectorSize;

     float* querySimilarities;

     int n = 0;

     MemoryContext  oldcontext;

     char *command;
     int ret;
     int proc;
     bool info;

     TopK topK;
     float maxDist;

     char* pqQuantizationTable = palloc(sizeof(char)*100);
     char* pqCodebookTable = palloc(sizeof(char)*100);

     start = clock();

     getTableName(PQ_QUANTIZATION, pqQuantizationTable, 100);
     getTableName(CODEBOOK, pqCodebookTable, 100);

     funcctx = SRF_FIRSTCALL_INIT ();
     oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

     k = PG_GETARG_INT32(1);

     // get codebook
     cb = getCodebook(&cbPositions, &cbCodes, pqCodebookTable);

     end = clock();
     elog(INFO,"get codebook time %f", (double) (end - start) / CLOCKS_PER_SEC);

    // read query from function args
    n = 0;
    convert_bytea_float4(PG_GETARG_BYTEA_P(0), &queryVector, &n);

    subvectorSize = n / cbPositions;

    // determine similarities of codebook entries to query vector
    querySimilarities = palloc(cbPositions*cbCodes*sizeof(float));
    getPrecomputedDistances(querySimilarities, cbPositions, cbCodes, subvectorSize, queryVector, cb);

    end = clock();
    elog(INFO,"calculate similarities time %f", (double) (end - start) / CLOCKS_PER_SEC);
    // calculate TopK by summing up squared distanced sum method
    topK = palloc(k*sizeof(TopKEntry));
    maxDist = 100.0; // sufficient high value
    for (int i = 0; i < k; i++){
      topK[i].distance = 100.0;
      topK[i].id = -1;
    }

    SPI_connect();
    command = palloc(sizeof(char)*100);
    sprintf(command, "SELECT id, vector FROM %s", pqQuantizationTable);
    elog(INFO, "command: %s", command);
    ret = SPI_exec(command, 0);
    proc = SPI_processed;
    end = clock();
    elog(INFO,"get quantization data time %f", (double) (end - start) / CLOCKS_PER_SEC);

    if (ret > 0 && SPI_tuptable != NULL){
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;

      Datum id;
      Datum vector;
      int16* codes;
      int wordId;
      float distance;

      int i;
      for (i = 0; i < proc; i++){

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);
        n = 0;
        convert_bytea_int16(DatumGetByteaP(vector), &codes, &n);
        // elog(INFO, "codes[0] %d", codes[0]);
        distance = 0;
        for (int j = 0; j < n; j++){
          distance += querySimilarities[j*cbCodes + codes[j]];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist);
          maxDist = topK[k-1].distance;
        }
      }
      SPI_finish();

    }
    end = clock();
    elog(INFO,"calculate distances time %f", (double) (end - start) / CLOCKS_PER_SEC);

    usrfctx = (UsrFctx*) palloc (sizeof (UsrFctx));
    fillUsrFctx(usrfctx, topK, k);
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (2 , false);
    TupleDescInitEntry (outtertupdesc,  1, "Id",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "Distance",FLOAT4OID,  -1, 0);
    slot = TupleDescGetSlot (outtertupdesc);
    funcctx -> slot = slot;
    attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
    funcctx -> attinmeta = attinmeta;

    MemoryContextSwitchTo (oldcontext);

    end = clock();
    elog(INFO,"time %f", (double) (end - start) / CLOCKS_PER_SEC);

  }

  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctx*) funcctx -> user_fctx;

  // return results
  if (usrfctx->iter >= usrfctx->k){
      SRF_RETURN_DONE (funcctx);
  }else{

    Datum result;
    HeapTuple outTuple;
    snprintf(usrfctx->values[0], 16, "%d", usrfctx->tk[usrfctx->iter].id);
    snprintf(usrfctx->values[1], 16, "%f", usrfctx->tk[usrfctx->iter].distance);
    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
  				      usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }

}

PG_FUNCTION_INFO_V1(ivfadc_search);

Datum
ivfadc_search(PG_FUNCTION_ARGS)
{

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctx *usrfctx;

  if (SRF_IS_FIRSTCALL ()){

    clock_t start;
    clock_t end;

    MemoryContext  oldcontext;

    Codebook residualCb;
    int cbPositions = 0;
    int cbCodes = 0;
    int subvectorSize;

    CoarseQuantizer cq;
    int cqSize;

    float4* queryVector;
    int k;
    int queryDim;

    float** residualVectors;

    int n = 0;

    float** querySimilarities;

    int ret;
    int proc;
    bool info;
    char* command;
    char* cur;

    TopK topK;
    float maxDist;

    // for coarse quantizer
    float minDist; // sufficient high value
    TopK cqSelection;

    int foundInstances;
    Blacklist bl;

    char* tableName = palloc(sizeof(char)*100);
    char* tableNameResidualCodebook = palloc(sizeof(char)*100);
    char* tableNameFineQuantization = palloc(sizeof(char)*100);

    int param_w;

    start = clock();

    getTableName(NORMALIZED, tableName, 100);
    getTableName(RESIDUAL_CODBOOK, tableNameResidualCodebook, 100);
    getTableName(RESIDUAL_QUANTIZATION, tableNameFineQuantization, 100);
    getParameter(PARAM_W, &param_w);

    residualVectors = palloc(sizeof(float*)*param_w);

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    k = PG_GETARG_INT32(1);

    // get codebook
    residualCb = getCodebook(&cbPositions, &cbCodes, tableNameResidualCodebook);
    // get coarse quantizer
    cq = getCoarseQuantizer(&cqSize);

    end = clock();
    elog(INFO,"get coarse quantizer and residual codebook data time %f", (double) (end - start) / CLOCKS_PER_SEC);

   // read query from function args
   n = 0;
   convert_bytea_float4(PG_GETARG_BYTEA_P(0), &queryVector, &n);
  //  queryVector = palloc(n*sizeof(float));
   queryDim = n;

   subvectorSize = n / cbPositions;

   foundInstances = 0;
   bl.isValid = false;

   topK = palloc(k*sizeof(TopKEntry));
   maxDist = 100.0; // sufficient high value
   for (int i = 0; i < k; i++){
     topK[i].distance = 100.0;
     topK[i].id = -1;
   }

   while (foundInstances < k){

     Blacklist* newBl;

     // get coarse_quantization(queryVector) (id)
     minDist = 1000.0;
     cqSelection = palloc(sizeof(TopKEntry)*param_w);
     for (int  i = 0; i < param_w; i++){
       cqSelection[i].distance = 100.0;
       cqSelection[i].id = -1;
     }
     for (int i=0; i < cqSize; i++){
       float dist;

       if (inBlacklist(i, &bl)){
         continue;
       }
       dist = squareDistance(queryVector, cq[i].vector, queryDim);
       if (dist < minDist){
         updateTopK(cqSelection, dist, i, param_w, minDist);
         minDist = cqSelection[param_w-1].distance;
       }
     }
     end = clock();
     elog(INFO,"determine coarse quantization time %f", (double) (end - start) / CLOCKS_PER_SEC);

     // add coarse quantization ids to Blacklist
     for (int i = 0; i < param_w; i++){
       newBl = palloc(sizeof(Blacklist));
       newBl->isValid = false;
       addToBlacklist(cqSelection[i].id, &bl, newBl);
     }

     // compute residual = queryVector - coarse_quantization(queryVector)
     for (int i = 0; i < param_w; i++){
       residualVectors[i] = palloc(queryDim*sizeof(float));
       for (int j = 0; j < queryDim; j++){
         residualVectors[i][j] = queryVector[j] - cq[cqSelection[i].id].vector[j];
       }
     }

     // compute subvector similarities lookup
     // determine similarities of codebook entries to residual vector
     querySimilarities = palloc(sizeof(float*)*cqSize);
     for (int j = 0; j < param_w; j++){
       int simId = cqSelection[j].id;
       querySimilarities[simId] = palloc(cbPositions*cbCodes*sizeof(float));
       getPrecomputedDistances(querySimilarities[simId], cbPositions, cbCodes, subvectorSize, residualVectors[j], residualCb);
     }

     end = clock();
     elog(INFO,"precalculate distances time %f", (double) (end - start) / CLOCKS_PER_SEC);

      // calculate TopK by summing up squared distanced sum method

      // connect to databse and compute approximated similarities with sum method
      SPI_connect();
      command = palloc((200+param_w*3)*sizeof(char));
      cur = command;
      cur += sprintf(command, "SELECT id, vector, coarse_id FROM %s WHERE coarse_id IN (", tableNameFineQuantization);
      for (int i = 0; i < param_w; i++){
        if (i != param_w - 1){
          cur += sprintf(cur, "%d, ", cqSelection[i].id);
        } else{
          cur += sprintf(cur, "%d)", cqSelection[i].id);
        }
      }
      // cq[cqId].id

      ret = SPI_exec(command, 0);
      proc = SPI_processed;
      end = clock();
      elog(INFO,"get quantization data time %f", (double) (end - start) / CLOCKS_PER_SEC);
      if (ret > 0 && SPI_tuptable != NULL){
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        int i;
        for (i = 0; i < proc; i++){
          Datum id;
          Datum vector;
          Datum coarseIdRaw;
          int16* codes;
          int wordId;
          int coarseId;
          float distance;

          HeapTuple tuple = tuptable->vals[i];
          id = SPI_getbinval(tuple, tupdesc, 1, &info);
          vector = SPI_getbinval(tuple, tupdesc, 2, &info);
          coarseIdRaw = SPI_getbinval(tuple, tupdesc, 3, &info);
          wordId = DatumGetInt32(id);
          coarseId = DatumGetInt32(coarseIdRaw);
          n = 0;
          convert_bytea_int16(DatumGetByteaP(vector), &codes, &n);
          distance = 0;
          for (int j = 0; j < n; j++){
            distance += querySimilarities[coarseId][j*cbCodes + codes[j]];
          }
          if (distance < maxDist){
            updateTopK(topK, distance, wordId, k, maxDist);
            maxDist = topK[k-1].distance;
          }
        }
        SPI_finish();
      }

      foundInstances += proc;

    }

    usrfctx = (UsrFctx*) palloc (sizeof (UsrFctx));
    fillUsrFctx(usrfctx, topK, k);
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (2 , false);
    TupleDescInitEntry (outtertupdesc,  1, "Id",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "Distance",FLOAT4OID,  -1, 0);
    slot = TupleDescGetSlot (outtertupdesc);
    funcctx -> slot = slot;
    attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
    funcctx -> attinmeta = attinmeta;

    MemoryContextSwitchTo (oldcontext);

    end = clock();
    elog(INFO,"total time %f", (double) (end - start) / CLOCKS_PER_SEC);

  }
  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctx*) funcctx -> user_fctx;

  // return results
  if (usrfctx->iter >= usrfctx->k){
      SRF_RETURN_DONE (funcctx);
  }else{
    Datum result;
    HeapTuple outTuple;
    snprintf(usrfctx->values[0], 16, "%d", usrfctx->tk[usrfctx->iter].id);
    snprintf(usrfctx->values[1], 16, "%f", usrfctx->tk[usrfctx->iter].distance);
    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
                usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }
}

PG_FUNCTION_INFO_V1(ivpq_search_in);

Datum
ivpq_search_in(PG_FUNCTION_ARGS)
{
    const float MAX_DIST = 1000.0;
    const int TOPK_BATCH_SIZE = 200;
    const int TARGET_LISTS_SIZE = 100;

    FuncCallContext *funcctx;
    TupleDesc        outtertupdesc;
    TupleTableSlot  *slot;
    AttInMetadata   *attinmeta;
    UsrFctxBatch *usrfctx;

    if (SRF_IS_FIRSTCALL ()){

      MemoryContext  oldcontext;

      // input parameter
      float4** queryVectors;
      int queryVectorsSize;
      int k;
      int* inputIds;
      int inputIdsSize;
      int* queryIds;

      int se_original; // size of search space is set to about SE*inputTermsSize vectors
      int se;
      int pvf; // post verification factor
      int method; // PQ / EXACT
      bool useTargetLists;
      float confidence;

      // search parameters
      int queryDim;
      int subvectorSize = 0;

      Codebook cb;
      int cbPositions = 0;
      int cbCodes = 0;
      const int DOUBLE_THRESHOLD = 50000;
      bool double_codes = false;
      int codesNumber = 0;

      #ifdef USE_MULTI_COARSE
      Codebook cqMulti;
      char* tableNameCQ = palloc(sizeof(char)*100);
      int cqCodes = 0;
      int cqPositions = 0;
      #endif
      #ifndef USE_MULTI_COARSE
      CoarseQuantizer cq;
      #endif

      int cqSize;

      float* statistics;

      // output variables
      TopK* topKs;
      float* maxDists;


      // time measurement
      clock_t start = 0;
      clock_t end = 0;
      clock_t last = 0;
      clock_t sub_start = 0;
      clock_t sub_end = 0 ;

      // helper variables
      int n = 0;
      Datum* queryIdData;

      int queryVectorsIndicesSize;
      int* queryVectorsIndices;

      TopKPV* topKPVs;
      int* fillLevels = NULL;

      TargetLists targetLists = NULL;
      int* targetCounts = NULL; // to determine if enough targets are observed

      // for coarse quantizer
      int** cqIdsMulti;
      int** cqTableIds;
      int* cqTableIdCounts;

      // for pq similarity calculation
      float4** querySimilarities = NULL;

      Datum* idsData;
      Datum *i_data; // for query vectors

      int ret;
      int proc;
      bool info;

      char* command;
      char* cur;

      char* tableName = palloc(sizeof(char)*100);
      char* tableNameCodebook = palloc(sizeof(char)*100);
      char* tableNameFineQuantizationIVPQ = palloc(sizeof(char)*100);


      elog(INFO, "start query");
      start = clock();
      last = clock();

      getTableName(NORMALIZED, tableName, 100);
      getTableName(CODEBOOK, tableNameCodebook, 100);
      getTableName(IVPQ_QUANTIZATION, tableNameFineQuantizationIVPQ, 100);

      funcctx = SRF_FIRSTCALL_INIT ();
      oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

      // get input parameter
      getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data, &n);
      queryVectors = palloc(n*sizeof(float4*));
      queryVectorsSize = n;
      for (int i = 0; i < n; i++){
        queryDim = 0;
        convert_bytea_float4(DatumGetByteaP(i_data[i]), &queryVectors[i], &queryDim);
      }
      n = 0;
      // for the output it is necessary to map query vectors to ids
      getArray(PG_GETARG_ARRAYTYPE_P(1), &queryIdData, &n);
      if (n != queryVectorsSize){
        elog(ERROR, "Number of query vectors and query vector ids differs!");
      }
      queryIds = palloc(queryVectorsSize*sizeof(int));
      for (int i=0; i< queryVectorsSize; i++){
        queryIds[i] = DatumGetInt32(queryIdData[i]);
      }
      n = 0;

      k = PG_GETARG_INT32(2);
      getArray(PG_GETARG_ARRAYTYPE_P(3), &idsData, &n); // target words
      inputIds = palloc(n*sizeof(int));

      for (int j=0; j< n; j++){
        inputIds[j] = DatumGetInt32(idsData[j]);
      }
      inputIdsSize = n;

      // parameter inputs
      se_original = PG_GETARG_INT32(4);
      pvf = PG_GETARG_INT32(5);
      method = PG_GETARG_INT32(6); // (0: PQ / 1: EXACT / 2: PQ with post verification)
      useTargetLists = PG_GETARG_BOOL(7);
      confidence = PG_GETARG_FLOAT4(8);
      se = se_original;

      queryVectorsIndicesSize = queryVectorsSize;
      queryVectorsIndices = palloc(sizeof(int)*queryVectorsSize);
      for (int i = 0; i < queryVectorsSize; i++){
        queryVectorsIndices[i] = i;
      }

      if ((method == PQ_CALC) || (method == PQ_PV_CALC)){
        // get codebook
        cb = getCodebook(&cbPositions, &cbCodes, tableNameCodebook);
        subvectorSize = queryDim / cbPositions;
      }
      // get coarse quantizer
      #ifdef USE_MULTI_COARSE
      getTableName(COARSE_QUANTIZATION, tableNameCQ, 100);
      cqMulti = getCodebook(&cqPositions, &cqCodes, tableNameCQ);
      cqSize = pow(cqCodes, cqPositions);
      #endif
      #ifndef USE_MULTI_COARSE
      cq = getCoarseQuantizer(&cqSize);
      #endif
      sub_start = clock();
      // get statistics about coarse centroid distribution
      statistics = getStatistics();
      sub_end = clock();
      elog(INFO, "TRACK get_statistics_time %f", (double) (sub_end - sub_start) / CLOCKS_PER_SEC);
      elog(INFO, "new iteration: se %d", se);
      // init topk data structures
      initTopKs(&topKs, &maxDists, queryVectorsSize, k, MAX_DIST);
      targetCounts = palloc(sizeof(int)*queryVectorsSize);
      if (method == PQ_PV_CALC){
        #ifdef OPT_FAST_PV_TOPK_UPDATE
          initTopKPVs(&topKPVs, &maxDists, queryVectorsSize, TOPK_BATCH_SIZE+k*pvf, MAX_DIST, queryDim);
          fillLevels = palloc(queryVectorsSize*sizeof(int));
          for (int i = 0; i < queryVectorsSize; i++){
            fillLevels[i] = 0;
          }
        #endif
        #ifndef OPT_FAST_PV_TOPK_UPDATE
          initTopKPVs(&topKPVs, &maxDists, queryVectorsSize, k*pvf, MAX_DIST, queryDim);
        #endif
        end = clock();
        elog(INFO, "TRACK setup_topkpv_time %f", (double) (end - last) / CLOCKS_PER_SEC);
      }

      if ((method == PQ_CALC) || (method == PQ_PV_CALC)){
        if (se*k > DOUBLE_THRESHOLD){
          double_codes = true;
        }else{
          double_codes = false;
        }
        // compute querySimilarities (precomputed distances) for product quantization
        if (double_codes){
          codesNumber = cbPositions/2;
          querySimilarities = palloc(sizeof(float4*)*queryVectorsSize);
          for (int i = 0; i < queryVectorsSize; i++){
            querySimilarities[i] = palloc(codesNumber*cbCodes*cbCodes*sizeof(float4));
            getPrecomputedDistancesDouble(querySimilarities[i], cbPositions, cbCodes, subvectorSize, queryVectors[i], cb);
          }

        }else{
          codesNumber = cbPositions;
          querySimilarities = palloc(sizeof(float4*)*queryVectorsSize);
          for (int i = 0; i < queryVectorsSize; i++){
            querySimilarities[i] = palloc(cbPositions*cbCodes*sizeof(float4));
            getPrecomputedDistances(querySimilarities[i], cbPositions, cbCodes, subvectorSize, queryVectors[i], cb);
          }
        }
      }

      end = clock();
      elog(INFO, "TRACK precomputation_time %f", (double) (end - last) / CLOCKS_PER_SEC);
      last = clock();

      proc = 0;
      while (queryVectorsIndicesSize > 0) {
        // compute coarse ids
        int coarse_ids_size = 0;
        int* blacklist = palloc(sizeof(int)*cqSize); // query should not contain coarse ids multiple times
        int* coarse_ids = palloc(sizeof(int)*cqSize);

        int newQueryVectorsIndicesSize;
        int* newQueryVectorsIndices;
        bool lastIteration;

        if ((useTargetLists) && (method != EXACT_CALC)){
          sub_start = clock();
          initTargetLists(&targetLists, queryVectorsSize, TARGET_LISTS_SIZE, method);
          sub_end = clock();
          elog(INFO, "TRACK init_targetlist_time %f", (double) (sub_end - sub_start) / CLOCKS_PER_SEC);
        }

        for (int i = 0; i < cqSize; i++){ // for construction of target query needed
          blacklist[i] = 0;
        }

        sub_start = clock();
        #ifdef USE_MULTI_COARSE
        lastIteration = determineCoarseIdsMultiWithStatisticsMulti(&cqIdsMulti, &cqTableIds, &cqTableIdCounts,
                          queryVectorsIndices,queryVectorsIndicesSize,queryVectorsSize,
                          MAX_DIST, cqMulti, cqSize, cqPositions, cqCodes, queryVectors, queryDim, statistics, inputIdsSize, (k*se), confidence);
        #endif
        #ifndef USE_MULTI_COARSE
        lastIteration = determineCoarseIdsMultiWithStatistics(&cqIdsMulti, &cqTableIds, &cqTableIdCounts,
                          queryVectorsIndices,queryVectorsIndicesSize,queryVectorsSize,
                          MAX_DIST, cq, cqSize, queryVectors, queryDim, statistics, inputIdsSize, (k*se), confidence);
        #endif
        sub_end = clock();
        elog(INFO, "TRACK determine_coarse_quantization_time %f", (double) (sub_end - sub_start) / CLOCKS_PER_SEC);


        for (int i = 0; i < cqSize; i++){
          if (cqTableIdCounts[i] > 0){
            coarse_ids[coarse_ids_size] = i;
            coarse_ids_size += 1;
          }
        }

        end = clock();
        command = palloc(inputIdsSize*100*sizeof(char) + 20*coarse_ids_size*sizeof(char)+500);
        cur = command;
        switch(method){
          case PQ_CALC:
            cur += sprintf(cur, "SELECT word_id, vector, coarse_id FROM %s WHERE (", tableNameFineQuantizationIVPQ);
            break;
          case PQ_PV_CALC:
            cur += sprintf(cur, "SELECT word_id, fq.vector, vecs.vector, coarse_id FROM %s AS fq INNER JOIN %s AS vecs ON fq.word_id = vecs.id WHERE (", tableNameFineQuantizationIVPQ, tableName);
            break;
          case EXACT_CALC:
            cur += sprintf(cur, "SELECT word_id, vecs.vector, coarse_id FROM %s AS fq INNER JOIN %s AS vecs ON fq.word_id = vecs.id WHERE (", tableNameFineQuantizationIVPQ, tableName);
            break;
          default:
            elog(ERROR, "Unknown computation method!");
        }
        // fill command
        cur += sprintf(cur, "(coarse_id IN ( ");
        for (int i = 0; i < coarse_ids_size; i++){
          if ( i != 0){
            cur += sprintf(cur, ",%d", coarse_ids[i]);
          }else{
            cur += sprintf(cur, "%d", coarse_ids[i]);
          }
        }
        cur += sprintf(cur, "))) AND (word_id IN (");
        for (int i = 0; i < inputIdsSize; i++){
          if ( i == inputIdsSize - 1){
              cur += sprintf(cur, "%d", inputIds[i]);
          }else{
            cur += sprintf(cur, "%d,", inputIds[i]);
          }
        }
        sprintf(cur, "))");
        end = clock();
        elog(INFO, "TRACK query_construction_time %f", (double) (end - last) / CLOCKS_PER_SEC);
        last = clock();
        SPI_connect();
        ret = SPI_execute(command, true, 0);
        end = clock();
        elog(INFO, "TRACK data_retrieval_time %f", (double) (end - last) / CLOCKS_PER_SEC);
        last = clock();
        proc = SPI_processed;
        elog(INFO, "TRACK retrieved %d results", proc);
        if (ret > 0 && SPI_tuptable != NULL){
          TupleDesc tupdesc = SPI_tuptable->tupdesc;
          SPITupleTable *tuptable = SPI_tuptable;
          int i;
          long counter = 0;
          float4* vector; // for post verification
          int offset = (method == PQ_PV_CALC) ? 4 : 3; // position offset for coarseIds
          // long* indices = palloc(sizeof(long)*cbPositions);
          int l;
          int16* codes2; // TODO später allokieren
          int codeRange = double_codes ? cbCodes*cbCodes : cbCodes;
          for (i = 0; i < proc; i++){
            int coarseId;
            int16* codes;
            int wordId;
            float distance;
            float* next;
            HeapTuple tuple = tuptable->vals[i];
            wordId = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &info));
            n = 0;
            if ((method == PQ_PV_CALC) || (method == PQ_CALC)){
              convert_bytea_int16(DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 2, &info)), &codes, &n);
              n = 0;
            }
            if (method == EXACT_CALC){
              convert_bytea_float4(DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 2, &info)), &vector, &n);
              n = 0;
            }
            if (method == PQ_PV_CALC){
              convert_bytea_float4(DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 3, &info)), &vector, &n);
              n = 0;
            }
            if (double_codes){
              codes2 = palloc(sizeof(int)*codesNumber);
              for (l = 0; l < codesNumber; l++){
                codes2[l] = codes[l*2] + codes[l*2+1]*cbCodes;
              }
            }else{
              codes2 = codes;
            }

            // read coarse ids
            coarseId = DatumGetInt32(SPI_getbinval(tuple, tupdesc, offset, &info));
            // calculate distances
            for (int j = 0; j < cqTableIdCounts[coarseId];j++){
              int queryVectorsIndex = cqTableIds[coarseId][j];
              targetCounts[queryVectorsIndex] += 1;
              if ((method == PQ_CALC) || (method == PQ_PV_CALC)){
                if (useTargetLists){
                  #ifdef OPT_PREFETCH
                  if (j % 25 == 0){
                    for (int n = 0; n < 50; n++){
                      if (j + n < cqTableIdCounts[coarseId]){
                        __builtin_prefetch(&cqTableIds[coarseId][j+n],0);
                        __builtin_prefetch(&targetLists[cqTableIds[coarseId][j+n]],0);
                        __builtin_prefetch(&(*(targetLists[cqTableIds[coarseId][j+n]]).last),1);
                      }
                    }
                  }
                  #endif /*OPT_PREFETCH*/
                  // add codes and word id to the target list which corresonds to the query
                  addToTargetList(targetLists, queryVectorsIndex, TARGET_LISTS_SIZE, method,codes2, vector, wordId);
                }else{

                  #ifdef OPT_PREFETCH
                  if (j % 100 == 0){
                    for (int n = 0; n < 200; n++){
                      if (j + n < cqTableIdCounts[coarseId]){
                        next = querySimilarities[cqTableIds[coarseId][j+n]];
                        for (l = 0; l < cbPositions; l++){
                          __builtin_prefetch(&next[cbCodes*l+ codes2[l]],0);
                        }
                      }
                    }
                  }
                  #endif /*OPT_PREFETCH*/

                  if (double_codes){
                    distance = computePQDistanceNew(querySimilarities[queryVectorsIndex], codes2, codesNumber, codeRange);
                  }else{
                    distance = computePQDistanceNew(querySimilarities[queryVectorsIndex], codes2, codesNumber, codeRange);
                  }
                  if (method == PQ_PV_CALC){
                    if (distance < maxDists[queryVectorsIndex]){
                      #ifdef OPT_FAST_PV_TOPK_UPDATE
                        updateTopKPVFast(topKPVs[queryVectorsIndex], TOPK_BATCH_SIZE+k*pvf, k*pvf, &fillLevels[queryVectorsIndex], &maxDists[queryVectorsIndex], queryDim, wordId, distance, vector);
                      #endif
                      #ifndef OPT_FAST_PV_TOPK_UPDATE
                        updateTopKPV(topKPVs[queryVectorsIndex], distance, wordId, k*pvf, maxDists[queryVectorsIndex], vector, queryDim);
                        maxDists[queryVectorsIndex] = topKPVs[queryVectorsIndex][k*pvf-1].distance;
                      #endif
                    }
                  }
                  if (method == PQ_CALC){
                    if (distance < maxDists[queryVectorsIndex]){
                      updateTopK(topKs[queryVectorsIndex], distance, wordId, k, maxDists[queryVectorsIndex]);
                      maxDists[queryVectorsIndex] = topKs[queryVectorsIndex][k-1].distance;
                    }
                  }
                }
              }else{
                // method == EXACT_CALC
                distance = squareDistance(queryVectors[queryVectorsIndex], vector, queryDim);
                if (distance < maxDists[queryVectorsIndex]){
                  updateTopK(topKs[queryVectorsIndex], distance, wordId, k, maxDists[queryVectorsIndex]);
                  maxDists[queryVectorsIndex] = topKs[queryVectorsIndex][k-1].distance;
                }
              }
            }
          }

          if (useTargetLists && ((method == PQ_CALC) || (method == PQ_PV_CALC))){
            elog(INFO, "TRACK reorder_time %f", (double) (clock() - last) / CLOCKS_PER_SEC);
          // calculate distances with target list
            for (i = 0; i < queryVectorsIndicesSize; i++){
              int queryVectorsIndex = queryVectorsIndices[i];
              TargetListElem* current = &targetLists[queryVectorsIndex];
              if (targetCounts[queryVectorsIndex] < k*se_original){
                targetCounts[queryVectorsIndex] = 0;
                continue;
              }
              while(current != NULL){
                for (int j = 0; j < current->size;j++){
                  float distance = 0;
                  // TODO use function
                  for (int l = 0; l < codesNumber; l++){
                    distance +=  querySimilarities[queryVectorsIndex][codeRange*l+ current->codes[j][l]];
                  }
                  if (method == PQ_PV_CALC){
                    if (distance < maxDists[queryVectorsIndex]){
                      #ifdef OPT_FAST_PV_TOPK_UPDATE
                        updateTopKPVFast(topKPVs[queryVectorsIndex], TOPK_BATCH_SIZE+k*pvf, k*pvf, &fillLevels[queryVectorsIndex], &maxDists[queryVectorsIndex], queryDim, current->ids[j], distance, current->vectors[j]);
                      #endif
                      #ifndef OPT_FAST_PV_TOPK_UPDATE
                        updateTopKPV(topKPVs[queryVectorsIndex], distance, current->ids[j], k*pvf, maxDists[queryVectorsIndex], current->vectors[j], queryDim);
                        maxDists[queryVectorsIndex] = topKPVs[queryVectorsIndex][k*pvf-1].distance;
                      #endif
                    }
                  }
                  if (method == PQ_CALC){
                    if (distance < maxDists[queryVectorsIndex]){
                      updateTopK(topKs[queryVectorsIndex], distance, current->ids[j], k, maxDists[queryVectorsIndex]);
                      maxDists[queryVectorsIndex] = topKs[queryVectorsIndex][k-1].distance;
                    }
                  }
                }
                current = current->next;
              }
            }
          }

          // post verification
          if (method == PQ_PV_CALC){
            #ifdef OPT_FAST_PV_TOPK_UPDATE
              sub_start = clock();
              for (int i = 0; i<queryVectorsIndicesSize;i++){
                int queryIndex = queryVectorsIndices[i];
                reorderTopKPV(topKPVs[queryIndex], k*pvf, &fillLevels[queryIndex], &maxDists[queryIndex]);
              }
              sub_end = clock();
              elog(INFO, "TRACK topkpv_reorder_time %f", (double) (sub_end - sub_start) / CLOCKS_PER_SEC);
            #endif
            sub_start = clock();
            postverify(queryVectorsIndices, queryVectorsIndicesSize, k, pvf, topKPVs, topKs, queryVectors, queryDim, MAX_DIST);
            sub_end = clock();
            elog(INFO, "TRACK pv_computation_time %f", (double) (sub_end - sub_start) / CLOCKS_PER_SEC);
          }

          end = clock();
          elog(INFO, "TRACK computation_time %f", (double) (end - last) / CLOCKS_PER_SEC);
          last = clock();
          elog(INFO, "finished computation counter %ld  ",counter);

        }
        SPI_finish();
        // recalcalculate queryIndices
        if (!lastIteration){
          newQueryVectorsIndicesSize = 0;
          newQueryVectorsIndices = palloc(sizeof(int)*queryVectorsIndicesSize);
          for (int i = 0; i < queryVectorsIndicesSize; i++){
            if (topKs[queryVectorsIndices[i]][k-1].distance == MAX_DIST){
              newQueryVectorsIndices[newQueryVectorsIndicesSize] = queryVectorsIndices[i];
              newQueryVectorsIndicesSize++;
              // empty topk
              initTopK(&topKs[queryVectorsIndices[i]], k, MAX_DIST);
              maxDists[queryVectorsIndices[i]] = MAX_DIST;
              if (method == PQ_PV_CALC){
                #ifdef OPT_FAST_PV_TOPK_UPDATE
                  initTopKPV(&topKPVs[queryVectorsIndices[i]], TOPK_BATCH_SIZE+k*pvf, MAX_DIST, queryDim);
                  fillLevels[i] = 0;
                #endif
                #ifndef OPT_FAST_PV_TOPK_UPDATE
                  initTopKPV(&topKPVs[queryVectorsIndices[i]], k*pvf, MAX_DIST, queryDim);
                #endif
              }

            }
          }
          queryVectorsIndicesSize = newQueryVectorsIndicesSize;
          queryVectorsIndices = newQueryVectorsIndices;
        }else{
          queryVectorsIndicesSize = 0;
        }
        end = clock();
        elog(INFO, "TRACK recalculate_query_indices_time %f", (double) (end - last) / CLOCKS_PER_SEC);
        last = clock();
        elog(INFO, "newQueryVectorsIndicesSize %d",newQueryVectorsIndicesSize);

        // claculate new se
        if (lastIteration){
          queryVectorsIndicesSize = 0;
        }
        se += se;

        elog(INFO, "se: %d queryVectorsIndicesSize: %d", se, queryVectorsIndicesSize);
      }
      // return tokKs
      usrfctx = (UsrFctxBatch*) palloc (sizeof (UsrFctxBatch));
      fillUsrFctxBatch(usrfctx, queryIds, queryVectorsSize, topKs, k);
      funcctx -> user_fctx = (void *)usrfctx;
      outtertupdesc = CreateTemplateTupleDesc (3 , false);

      TupleDescInitEntry (outtertupdesc,  1, "QueryId",    INT4OID, -1, 0);
      TupleDescInitEntry (outtertupdesc,  2, "TargetId",    INT4OID, -1, 0);
      TupleDescInitEntry (outtertupdesc,  3, "Distance",FLOAT4OID,  -1, 0);
      slot = TupleDescGetSlot (outtertupdesc);
      funcctx -> slot = slot;
      attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
      funcctx -> attinmeta = attinmeta;
      end = clock();
      elog(INFO, "TRACK total_time %f", (double) (end - start) / CLOCKS_PER_SEC);
      MemoryContextSwitchTo (oldcontext);

    }
    funcctx = SRF_PERCALL_SETUP ();
    usrfctx = (UsrFctxBatch*) funcctx -> user_fctx;
    // return results
    if (usrfctx->iter >= usrfctx->k * usrfctx->queryIdsSize){
        SRF_RETURN_DONE (funcctx);
    }else{
      Datum result;
      HeapTuple outTuple;
      snprintf(usrfctx->values[0], 16, "%d", usrfctx->queryIds[usrfctx->iter / usrfctx->k]);
      snprintf(usrfctx->values[1], 16, "%d", usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].id);
      snprintf(usrfctx->values[2], 16, "%f", usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].distance);
      usrfctx->iter++;
      outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
                  usrfctx -> values);
      result = TupleGetDatum (funcctx -> slot, outTuple);
      SRF_RETURN_NEXT(funcctx, result);

    }

}

PG_FUNCTION_INFO_V1(pq_search_in_batch);

Datum
pq_search_in_batch(PG_FUNCTION_ARGS)
{

  const float MAX_DIST = 1000.0;
  const int TARGET_LISTS_SIZE = 1000;

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctxBatch *usrfctx;

  if (SRF_IS_FIRSTCALL ()){

    MemoryContext  oldcontext;

    // input parameter
    float4** queryVectors;
    int queryVectorsSize;
    int k;
    int* inputIds;
    int inputIdsSize;
    int* queryIds;

    TargetListElem targetList;

    int queryDim;
    int subvectorSize = 0;

    Codebook cb;
    int cbPositions = 0;
    int cbCodes = 0;
    // for pq similarity calculation
    float4** querySimilarities = NULL;

    // output variables
    TopK* topKs;
    float* maxDists;

    // time measurement
    clock_t start = 0;
    clock_t end = 0;
    clock_t last = 0;
    clock_t sub_start = 0;
    clock_t sub_end = 0 ;

    int ret;
    int proc;
    bool info;

    char* command;
    char* cur;
    start = clock();
    last = clock();

    bool useTargetLists;

    // helper variables
    int n = 0;
    Datum* queryIdData;
    Datum* idsData;
    Datum *i_data; // for query vectors

    char* tableName = palloc(sizeof(char)*100);
    char* tableNameCodebook = palloc(sizeof(char)*100);
    char* tableNameFineQuantization = palloc(sizeof(char)*100);

    elog(INFO, "start query");
    start = clock();
    last = clock();

    getTableName(NORMALIZED, tableName, 100);
    getTableName(CODEBOOK, tableNameCodebook, 100);
    getTableName(PQ_QUANTIZATION, tableNameFineQuantization, 100);

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    // get input parameter
    getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data, &n);
    queryVectors = palloc(n*sizeof(float4*));
    queryVectorsSize = n;
    for (int i = 0; i < n; i++){
      queryDim = 0;
      convert_bytea_float4(DatumGetByteaP(i_data[i]), &queryVectors[i], &queryDim);
    }
    n = 0;
    // for the output it is necessary to map query vectors to ids
    getArray(PG_GETARG_ARRAYTYPE_P(1), &queryIdData, &n);
    if (n != queryVectorsSize){
      elog(ERROR, "Number of query vectors and query vector ids differs!");
    }
    queryIds = palloc(queryVectorsSize*sizeof(int));
    for (int i=0; i< queryVectorsSize; i++){
      queryIds[i] = DatumGetInt32(queryIdData[i]);
    }
    n = 0;

    k = PG_GETARG_INT32(2);
    getArray(PG_GETARG_ARRAYTYPE_P(3), &idsData, &n); // target words
    inputIds = palloc(n*sizeof(int));

    for (int j=0; j< n; j++){
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdsSize = n;

    useTargetLists = PG_GETARG_BOOL(4);

    cb = getCodebook(&cbPositions, &cbCodes, tableNameCodebook);
    subvectorSize = queryDim / cbPositions;

    initTopKs(&topKs, &maxDists, queryVectorsSize, k, MAX_DIST);

    querySimilarities = palloc(sizeof(float4*)*queryVectorsSize);
    for (int i = 0; i < queryVectorsSize; i++){
      querySimilarities[i] = palloc(cbPositions*cbCodes*sizeof(float4));
      getPrecomputedDistances(querySimilarities[i], cbPositions, cbCodes, subvectorSize, queryVectors[i], cb);
    }

    end = clock();
    elog(INFO, "TRACK precomputation_time %f", (double) (end - last) / CLOCKS_PER_SEC);
    last = clock();

    if (useTargetLists){
      sub_start = clock();
      targetList.codes = palloc(sizeof(int16*)*TARGET_LISTS_SIZE);
      targetList.ids = palloc(sizeof(int)*TARGET_LISTS_SIZE);
      targetList.size = 0;
      targetList.next = NULL;
      targetList.last = &targetList;
      sub_end = clock();
      elog(INFO, "TRACK init_targetlist_time %f", (double) (sub_end - sub_start) / CLOCKS_PER_SEC);
    }

    command = palloc(inputIdsSize*100*sizeof(char) +1000);
    cur = command;
    cur += sprintf(cur, "SELECT id, vector FROM %s WHERE id IN (", tableNameFineQuantization);
    for (int i = 0; i < inputIdsSize; i++){
      if ( i == inputIdsSize - 1){
          cur += sprintf(cur, "%d", inputIds[i]);
      }else{
        cur += sprintf(cur, "%d,", inputIds[i]);
      }
    }
    sprintf(cur, ")");

    end = clock();
    elog(INFO, "TRACK query_construction_time %f", (double) (end - last) / CLOCKS_PER_SEC);
    last = clock();
    SPI_connect();
    ret = SPI_execute(command, true, 0);
    end = clock();
    elog(INFO, "TRACK data_retrieval_time %f", (double) (end - last) / CLOCKS_PER_SEC);
    last = clock();
    proc = SPI_processed;
    elog(INFO, "retrieved %d results", proc);
    if (ret > 0 && SPI_tuptable != NULL){
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;

      for (int i = 0; i < proc; i++){
        int16* codes;
        int wordId;
        float distance;
        float* next;
        TargetListElem* currentTargetList = targetList.last;
        HeapTuple tuple = tuptable->vals[i];
        wordId = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &info));
        n = 0;
        convert_bytea_int16(DatumGetByteaP(SPI_getbinval(tuple, tupdesc, 2, &info)), &codes, &n);
        n = 0;
        if (useTargetLists){
          // add codes and word id to the target list
          currentTargetList->codes[currentTargetList->size] = codes;
          currentTargetList->ids[currentTargetList->size] = wordId;
          currentTargetList->size += 1;
          if (currentTargetList->size == TARGET_LISTS_SIZE){
            currentTargetList->next = palloc(sizeof(TargetListElem));
            currentTargetList->next->codes = palloc(sizeof(int16*)*TARGET_LISTS_SIZE);
            currentTargetList->next->ids = palloc(sizeof(int)*TARGET_LISTS_SIZE);
            currentTargetList->next->size = 0;
            currentTargetList->next->next = NULL;
            targetList.last = currentTargetList->next;
          }
        }else{
          for (int j = 0; j < queryVectorsSize; j++){
            distance = computePQDistanceNew(querySimilarities[j], codes, cbPositions, cbCodes);
            if (distance < maxDists[j]){
              updateTopK(topKs[j], distance, wordId, k, maxDists[j]);
              maxDists[j] = topKs[j][k-1].distance;
            }
          }
        }
      }
    }

    if (useTargetLists){
      for (int i = 0; i < queryVectorsSize; i++){
        TargetListElem* current = &targetList;
        while(current != NULL){
          for (int j = 0; j < current->size;j++){
            float distance = 0;
            for (int l = 0; l < cbPositions; l++){
              distance +=  querySimilarities[i][cbCodes*l+ current->codes[j][l]];
            }
            if (distance < maxDists[i]){
              updateTopK(topKs[i], distance, current->ids[j], k, maxDists[i]);
              maxDists[i] = topKs[i][k-1].distance;
            }
          }
          current = current->next;
        }
      }
    }
    end = clock();
    elog(INFO, "TRACK computation_time %f", (double) (end - last) / CLOCKS_PER_SEC);
    last = clock();

    SPI_finish();

    // return tokKs
    usrfctx = (UsrFctxBatch*) palloc (sizeof (UsrFctxBatch));
    fillUsrFctxBatch(usrfctx, queryIds, queryVectorsSize, topKs, k);
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (3 , false);

    TupleDescInitEntry (outtertupdesc,  1, "QueryId",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "TargetId",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  3, "Distance",FLOAT4OID,  -1, 0);
    slot = TupleDescGetSlot (outtertupdesc);
    funcctx -> slot = slot;
    attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
    funcctx -> attinmeta = attinmeta;
    end = clock();
    elog(INFO, "TRACK total_time %f", (double) (end - start) / CLOCKS_PER_SEC);
    MemoryContextSwitchTo (oldcontext);

  }
  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctxBatch*) funcctx -> user_fctx;
  // return results
  if (usrfctx->iter >= usrfctx->k * usrfctx->queryIdsSize){
      SRF_RETURN_DONE (funcctx);
  }else{
    Datum result;
    HeapTuple outTuple;
    snprintf(usrfctx->values[0], 16, "%d", usrfctx->queryIds[usrfctx->iter / usrfctx->k]);
    snprintf(usrfctx->values[1], 16, "%d", usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].id);
    snprintf(usrfctx->values[2], 16, "%f", usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].distance);
    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
                usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }
}

PG_FUNCTION_INFO_V1(ivfadc_batch_search);

Datum ivfadc_batch_search(PG_FUNCTION_ARGS){
  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctxBatch *usrfctx;

  if (SRF_IS_FIRSTCALL ()){

    clock_t start;
    clock_t end;

    MemoryContext  oldcontext;

    Datum* queryData;

    Codebook residualCb;
    int cbPositions = 0;
    int cbCodes = 0;
    int subvectorSize;

    CoarseQuantizer cq;
    int cqSize;

    int* queryIds;
    int queryIdsSize;
    float4** queryVectors = NULL;
    int queryVectorsSize = 0;
    int* idArray = NULL;
    int k;
    int queryDim = 0;

    float4* residualVector;

    int n = 0;

    float4** querySimilarities;

    int ret;
    int proc;
    bool info;
    char* command;
    char* cur;

    TopK* topKs;
    float4* maxDists;

    // for coarse quantizer
    float4 minDist; // sufficient high value
    int* cqIds;
    int** cqTableIds;
    int* cqTableIdCounts;

    int* foundInstances;
    Blacklist* bls;
    bool finished;

    char* tableName = palloc(sizeof(char)*100);
    char* tableNameResidualCodebook = palloc(sizeof(char)*100);
    char* tableNameFineQuantization = palloc(sizeof(char)*100);

    start = clock();

    getTableName(NORMALIZED, tableName, 100);
    getTableName(RESIDUAL_CODBOOK, tableNameResidualCodebook, 100);
    getTableName(RESIDUAL_QUANTIZATION, tableNameFineQuantization, 100);

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    k = PG_GETARG_INT32(1);


    // get codebook
    residualCb = getCodebook(&cbPositions, &cbCodes, tableNameResidualCodebook);

    // get coarse quantizer
    cq = getCoarseQuantizer(&cqSize);

    end = clock();
    elog(INFO,"get coarse quantizer data time %f", (double) (end - start) / CLOCKS_PER_SEC);

    // read query from function args
    getArray(PG_GETARG_ARRAYTYPE_P(0), &queryData, &n);
    queryIdsSize = n;
    queryIds = palloc(queryIdsSize*sizeof(int));
    for (int i=0; i< queryIdsSize; i++){
      queryIds[i] = DatumGetInt32(queryData[i]);
    }

    end = clock();
    elog(INFO,"get query ids time %f", (double) (end - start) / CLOCKS_PER_SEC);

    // read out vectors for ids
    SPI_connect();
    command = palloc((100 + queryIdsSize*10)*sizeof(char));
    cur = command;
    cur += sprintf(command, "SELECT id, vector FROM %s WHERE id IN (", tableName);
    for (int i = 0; i < queryIdsSize; i++){
      if (i != (queryIdsSize - 1)){
        cur += sprintf(cur, "%d,", queryIds[i]);
      }else{
        cur += sprintf(cur, "%d)", queryIds[i]);
      }
    }
   ret = SPI_exec(command, 0);
   proc = SPI_processed;
   if (ret > 0 && SPI_tuptable != NULL){
     TupleDesc tupdesc = SPI_tuptable->tupdesc;
     SPITupleTable *tuptable = SPI_tuptable;
     queryVectorsSize = proc;
     queryVectors = SPI_palloc(sizeof(float4*)*queryVectorsSize);
     idArray = SPI_palloc(sizeof(int)*queryVectorsSize);
     for (int i = 0; i < proc; i++){
       Datum id;
       Datum vector;
       bytea* data;

       HeapTuple tuple = tuptable->vals[i];
       id = SPI_getbinval(tuple, tupdesc, 1, &info);
       vector = SPI_getbinval(tuple, tupdesc, 2, &info);
       idArray[i] = DatumGetInt32(id);

       data = DatumGetByteaP(vector);
       queryDim = (VARSIZE(data) - VARHDRSZ) / sizeof(float4);
       queryVectors[i] = SPI_palloc(queryDim * sizeof(float4));
       memcpy(queryVectors[i], (float4*) VARDATA(data), queryDim*sizeof(float4));
     }
     SPI_finish();
   }

   end = clock();
   elog(INFO,"get vectors for ids time %f", (double) (end - start) / CLOCKS_PER_SEC);

   subvectorSize = queryDim / cbPositions;

   foundInstances = palloc(sizeof(int)*queryVectorsSize);
   topKs = palloc(sizeof(TopK)*queryVectorsSize);
   bls = palloc(sizeof(Blacklist)*queryVectorsSize);
   cqIds = palloc(sizeof(int)*queryVectorsSize);
   querySimilarities = palloc(sizeof(float4*)*queryVectorsSize);
   maxDists = palloc(sizeof(float4)*queryVectorsSize);
   for (int i = 0; i < queryVectorsSize; i++){
     foundInstances[i] = 0;
     bls[i].isValid = false;
     topKs[i] = palloc(k*sizeof(TopKEntry));
     for (int j = 0; j < k; j++){
       topKs[i][j].distance = 100.0; // sufficient high value
       topKs[i][j].id = -1;
     }
     cqIds[i] = -1;
     maxDists[i] = 100;
   }
   end = clock();
   elog(INFO,"allocate memory time %f", (double) (end - start) / CLOCKS_PER_SEC);

   finished = false;

   while (!finished){
     cqTableIds = palloc(sizeof(int*)*cqSize); // stores all coarseQuantizationIds
     cqTableIdCounts = palloc(sizeof(int)*cqSize);
     for (int i = 0; i < cqSize; i++){
       cqTableIds[i] = NULL;
       cqTableIdCounts[i] = 0;
     }

     finished = true;
     for (int i = 0; i < queryVectorsSize; i++){ // determine coarse quantizations (and residual vectors)
       if (foundInstances[i] >= k){
         continue;
       }

       Blacklist* newBl;

       // get coarse_quantization(queryVector) (id)
       minDist = 1000;
       for (int j=0; j < cqSize; j++){
         float4 dist;

         if (inBlacklist(j, &(bls[i]))){
           continue;
         }
         dist = squareDistance(queryVectors[i], cq[j].vector, queryDim);
         if (dist < minDist){
           minDist = dist;
           cqIds[i] = j;
         }

       }

       // add coarse quantizer to Blacklist
       newBl = palloc(sizeof(Blacklist));
       newBl->isValid = false;

       addToBlacklist(cqIds[i], &(bls[i]), newBl);
       cqTableIdCounts[cq[cqIds[i]].id] += 1;

       // compute residual = queryVector - coarse_quantization(queryVector)
       residualVector = palloc(queryDim*sizeof(float4));
       for (int j = 0; j < queryDim; j++){
         residualVector[j] = queryVectors[i][j] - cq[cqIds[i]].vector[j];
       }
       // compute subvector similarities lookup
       // determine similarities of codebook entries to residual vector
       querySimilarities[i] = palloc(cbPositions*cbCodes*sizeof(float4));
       getPrecomputedDistances(querySimilarities[i], cbPositions, cbCodes, subvectorSize, residualVector, residualCb);
     }

     end = clock();
     elog(INFO,"precompute distances time %f", (double) (end - start) / CLOCKS_PER_SEC);

     // create cqTableIds lookup -> maps coarse_ids to positions of queryVectors in queryVectors
     for (int i = 0; i < cqSize; i++){
       if (cqTableIdCounts > 0){
         cqTableIds[i] = palloc(sizeof(int)*cqTableIdCounts[i]);
         for (int j = 0; j < cqTableIdCounts[i]; j++){
           cqTableIds[i][j] = 0;
         }
       }
     }
     for (int i = 0; i < queryVectorsSize; i++){
       if (foundInstances[i] >= k){
         continue;
       }
       int j = 0;
       while(cqTableIds[cqIds[i]][j]){
         j++;
       }
       cqTableIds[cqIds[i]][j] = i;
     }

     // determine fine quantization and calculate distances
     SPI_connect();
     command = palloc((200+queryVectorsSize*10)*sizeof(char));
     cur = command;
     cur += sprintf(command, "SELECT id, vector, coarse_id FROM %s WHERE coarse_id IN(", tableNameFineQuantization);
     for (int i = 0; i < queryVectorsSize; i++){
       if (foundInstances[i] >= k){
         continue;
       }
       cur += sprintf(cur, "%d,", cq[cqIds[i]].id);
     }
     sprintf(cur-1, ")");
     end = clock();
     elog(INFO,"create command time %f", (double) (end - start) / CLOCKS_PER_SEC);
     ret = SPI_exec(command, 0);
     proc = SPI_processed;
     end = clock();
     elog(INFO,"get quantization data time %f", (double) (end - start) / CLOCKS_PER_SEC);
     if (ret > 0 && SPI_tuptable != NULL){
       TupleDesc tupdesc = SPI_tuptable->tupdesc;
       SPITupleTable *tuptable = SPI_tuptable;
       for (int i = 0; i < proc; i++){
         Datum id;
         Datum vector;
         Datum coarseIdData;
         bytea* data;
         int16* vector_raw;
         int wordId;
         int coarseId;
         float4 distance;

         HeapTuple tuple = tuptable->vals[i];
         id = SPI_getbinval(tuple, tupdesc, 1, &info);
         vector = SPI_getbinval(tuple, tupdesc, 2, &info);
         coarseIdData = SPI_getbinval(tuple, tupdesc, 3, &info);
         wordId = DatumGetInt32(id);
         coarseId = DatumGetInt32(coarseIdData);
         data = DatumGetByteaP(vector);
         vector_raw = (int16*) VARDATA(data);

         for (int j = 0; j < cqTableIdCounts[coarseId];j++){
           int queryVectorsIndex = cqTableIds[coarseId][j];
           distance = 0;
           for (int l = 0; l < cbPositions; l++){
             int code = vector_raw[l];
             distance += querySimilarities[queryVectorsIndex][l*cbCodes + code];
           }
           if (distance < maxDists[queryVectorsIndex]){
             updateTopK(topKs[queryVectorsIndex], distance, wordId, k, maxDists[queryVectorsIndex]);
             maxDists[queryVectorsIndex] = topKs[queryVectorsIndex][k-1].distance;
             foundInstances[queryVectorsIndex]++;
           }
         }
       }
       SPI_finish();
     }
     for (int i = 0; i < queryVectorsSize; i++){
       if (foundInstances[i] < k){
         finished = false;
       }
     }

   }

    // return tokKs
    usrfctx = (UsrFctxBatch*) palloc (sizeof (UsrFctxBatch));
    fillUsrFctxBatch(usrfctx, idArray, queryVectorsSize, topKs, k);
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (3 , false);

    TupleDescInitEntry (outtertupdesc,  1, "QueryId",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "Id",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  3, "Distance",FLOAT4OID,  -1, 0);
    slot = TupleDescGetSlot (outtertupdesc);
    funcctx -> slot = slot;
    attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
    funcctx -> attinmeta = attinmeta;

    MemoryContextSwitchTo (oldcontext);

    end = clock();
    elog(INFO,"total time %f", (double) (end - start) / CLOCKS_PER_SEC);

  }
  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctxBatch*) funcctx -> user_fctx;

  // return results
  if (usrfctx->iter >= usrfctx->k * usrfctx->queryIdsSize){
      SRF_RETURN_DONE (funcctx);
      elog(INFO, "deleted it");
  }else{
    Datum result;
    HeapTuple outTuple;
    snprintf(usrfctx->values[0], 16, "%d", usrfctx->queryIds[usrfctx->iter / usrfctx->k]);
    snprintf(usrfctx->values[1], 16, "%d", usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].id);
    snprintf(usrfctx->values[2], 16, "%f", usrfctx->tk[usrfctx->iter / usrfctx->k][usrfctx->iter % usrfctx->k].distance);
    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
                usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }
}

PG_FUNCTION_INFO_V1(pq_search_in);

Datum
pq_search_in(PG_FUNCTION_ARGS)
{

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctx *usrfctx;

  if (SRF_IS_FIRSTCALL ())
   {


    int k;

    float* queryVector;
    int queryVectorSize;

    Datum* idsData;
    int* inputIds;
    int inputIdSize;

    int n = 0;

    Codebook cb;
    int cbPositions = 0;
    int cbCodes = 0;
    int subvectorSize;

    float* querySimilarities;
    TopK topK;
    float maxDist;

    int ret;
    int proc;
    bool info;
    char* command;
    char * cur;

    MemoryContext  oldcontext;

    char* tableNameCodebook = palloc(sizeof(char)*100);
    char* tableNamePqQuantization = palloc(sizeof(char)*100);

    getTableName(CODEBOOK, tableNameCodebook, 100);
    getTableName(PQ_QUANTIZATION, tableNamePqQuantization, 100);


    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    k = PG_GETARG_INT32(1);

    // read query from function args
    convert_bytea_float4(PG_GETARG_BYTEA_P(0), &queryVector, &n);
    queryVectorSize = n;

    // read ids from function args
    getArray(PG_GETARG_ARRAYTYPE_P(2), &idsData, &n);
    inputIds = palloc(n*sizeof(float));
    for (int j=0; j< n; j++){
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdSize = n;

    // get pq codebook
    cb = getCodebook(&cbPositions, &cbCodes, tableNameCodebook);

    subvectorSize = queryVectorSize / cbPositions;

    // determine similarities of codebook entries to query vector
    querySimilarities = palloc(cbPositions*cbCodes*sizeof(float));
    getPrecomputedDistances(querySimilarities, cbPositions, cbCodes, subvectorSize, queryVector, cb);

    // calculate TopK by summing up squared distanced sum method
    topK = palloc(k*sizeof(TopKEntry));
    maxDist = 1000.0; // sufficient high value
    for (int i = 0; i < k; i++){
      topK[i].distance = 1000.0;
      topK[i].id = -1;
    }
    // get codes for all entries with an id in inputIds -> SQL Query
    SPI_connect();
    command = palloc(60* sizeof(char) + inputIdSize*10*sizeof(char));
    sprintf(command, "SELECT id, vector FROM %s WHERE id IN (", tableNamePqQuantization);
    // fill command
    cur = command + strlen(command);
    for (int i = 0; i < inputIdSize; i++){
      if ( i == inputIdSize - 1){
          cur += sprintf(cur, "%d", inputIds[i]);
      }else{
        cur += sprintf(cur, "%d, ", inputIds[i]);
      }
    }
    cur += sprintf(cur, ")");
    ret = SPI_exec(command, 0);
    proc = SPI_processed;
    if (ret > 0 && SPI_tuptable != NULL){
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;
      int i;
      for (i = 0; i < proc; i++){
        Datum id;
        Datum vector;
        int16* codes;
        int wordId;
        float distance;

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);

        n = 0;
        convert_bytea_int16(DatumGetByteaP(vector), &codes, &n);

        distance = 0;
        for (int j = 0; j < n; j++){
          distance += querySimilarities[j*cbCodes + codes[j]];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist);
          maxDist = topK[k-1].distance;
        }
      }
      SPI_finish();

      usrfctx = (UsrFctx*) palloc (sizeof (UsrFctx));
      fillUsrFctx(usrfctx, topK, k);
      funcctx -> user_fctx = (void *)usrfctx;
      outtertupdesc = CreateTemplateTupleDesc (2 , false);
      TupleDescInitEntry (outtertupdesc,  1, "Id",    INT4OID, -1, 0);
      TupleDescInitEntry (outtertupdesc,  2, "Distance",FLOAT4OID,  -1, 0);
      slot = TupleDescGetSlot (outtertupdesc);
      funcctx -> slot = slot;
      attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
      funcctx -> attinmeta = attinmeta;

      MemoryContextSwitchTo (oldcontext);

    }

  }
  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctx*) funcctx -> user_fctx;

  // return results
  if (usrfctx->iter >= usrfctx->k){
      SRF_RETURN_DONE (funcctx);
  }else{

    Datum result;
    HeapTuple outTuple;
    snprintf(usrfctx->values[0], 16, "%d", usrfctx->tk[usrfctx->iter].id);
    snprintf(usrfctx->values[1], 16, "%f", usrfctx->tk[usrfctx->iter].distance);
    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
  				      usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }

}

PG_FUNCTION_INFO_V1(cluster_pq);

Datum
cluster_pq(PG_FUNCTION_ARGS)
{
  // input: array of ids to cluster, number of clusters
  // output: set of cluster vectors -> arrays of ids corresponding to cluster vectors

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctxCluster *usrfctx;
  int vectorSize = 300; // TODO change this

  const int DATASET_SIZE = 3000000; // TODO get this dynamically

  if (SRF_IS_FIRSTCALL ()){

    int n = 0;

    MemoryContext  oldcontext;

    Datum* idsData;

    int* inputIds;
    int inputIdsSize;

    int* kmCentroidIds;

    int k;
    int iterations;

    float** querySimilarities;

    Codebook cb;
    int cbPositions = 0;
    int cbCodes = 0;
    int subvectorSize;

    // data structure for relation id -> nearest centroid
    int* nearestCentroid;

    // store number of nearest vectors per centroid
    int* relationCounts;

    // randomly choosen init vectors for centroids
    WordVectors idVectors;

    // centroids
    float** kmCentroids;

    // unnormalized new centroids
    float** kmCentroidsNew;

    char* tableName;
    char* tableNameCodebook = palloc(sizeof(char)*100);
    char* tableNamePqQuantization = palloc(sizeof(char)*100);
    getTableName(CODEBOOK, tableNameCodebook, 100);
    getTableName(PQ_QUANTIZATION, tableNamePqQuantization, 100);

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    k = PG_GETARG_INT32(1);

    iterations = 10;

    relationCounts = palloc(sizeof(int)*k);

    nearestCentroid = palloc(sizeof(int)*(DATASET_SIZE+1));
    // read ids from function args
    getArray(PG_GETARG_ARRAYTYPE_P(0), &idsData, &n);

    inputIds = palloc(n*sizeof(int));
    for (int j=0; j< n; j++){
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdsSize = n;

    if (inputIdsSize < k){
      elog(ERROR, "|ids| < k");
      SRF_RETURN_DONE (funcctx);
    }
    // get pq codebook
    cb = getCodebook(&cbPositions, &cbCodes, tableNameCodebook);
    subvectorSize = vectorSize / cbPositions;
    // choose initial km-centroid randomly
    kmCentroidIds = palloc(sizeof(int)*k);
    shuffle(inputIds, kmCentroidIds, inputIdsSize, k);
    // get vectors for ids
    tableName = palloc(sizeof(char)*100);
    getTableName(NORMALIZED, tableName, 100);
    idVectors = getVectors(tableName, kmCentroidIds, k);
    kmCentroids = idVectors.vectors;
    kmCentroidsNew = palloc(sizeof(float*)*k);
    for (int i = 0; i < k; i++){
      kmCentroidsNew[i] = palloc(sizeof(float)*vectorSize);
    }

    for (int iteration = 0; iteration < iterations; iteration++){

      int ret;
      int proc;
      bool info;
      char* command;
      char * cur;

      // init kmCentroidsNew
      for (int i=0; i<k;i++){
        for (int j = 0; j < vectorSize; j++){
          kmCentroidsNew[i][j] = 0;
        }
      }
      // determine similarities of codebook entries to km_centroid vector
      querySimilarities = palloc(sizeof(float*) * k);

      for (int cs = 0; cs < k; cs++){
        querySimilarities[cs] = palloc(cbPositions*cbCodes*sizeof(float));
        getPrecomputedDistances(querySimilarities[cs], cbPositions, cbCodes, subvectorSize, kmCentroids[cs], cb);
      }


      // reset counts for relation
      for (int i = 0; i < k; i++){
        relationCounts[i] = 0;
      }

      // get vectors for ids
      // get codes for all entries with an id in inputIds -> SQL Query
      SPI_connect();

      command = palloc(200* sizeof(char) + inputIdsSize*8*sizeof(char));
      sprintf(command, "SELECT pq_quantization.id, pq_quantization.vector, vecs.vector FROM %s AS pq_quantization INNER JOIN %s AS vecs ON vecs.id = pq_quantization.id WHERE pq_quantization.id IN (", tableNamePqQuantization, tableName);
      // fill command
      cur = command + strlen(command);
      for (int i = 0; i < inputIdsSize; i++){
        if ( i == inputIdsSize - 1){
            cur += sprintf(cur, "%d", inputIds[i]);
        }else{
          cur += sprintf(cur, "%d, ", inputIds[i]);
        }
      }
      cur += sprintf(cur, ")");

      ret = SPI_exec(command, 0);
      proc = SPI_processed;
      if (ret > 0 && SPI_tuptable != NULL){
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        int i;
        for (i = 0; i < proc; i++){
          Datum id;
          Datum pqVector;
          Datum bigVector;

          int16* dataPqVector;
          float4* dataBigVector;

          int wordId;
          float distance;
          int pqSize;

          // variables to determine best match
          float minDist = 100; // sufficient high value

          HeapTuple tuple = tuptable->vals[i];
          id = SPI_getbinval(tuple, tupdesc, 1, &info);
          pqVector = SPI_getbinval(tuple, tupdesc, 2, &info);
          bigVector = SPI_getbinval(tuple, tupdesc, 3, &info);

          wordId = DatumGetInt32(id);
          n = 0;
          convert_bytea_int16(DatumGetByteaP(pqVector), &dataPqVector, &n);
          pqSize = n;

          n = 0;
          convert_bytea_float4(DatumGetByteaP(bigVector), &dataBigVector, &n);

          for (int centroidIndex = 0; centroidIndex < k; centroidIndex++){
            distance = 0;
            for (int j = 0; j < pqSize; j++){
              int16 code = dataPqVector[j];
              distance += querySimilarities[centroidIndex][j*cbCodes + code];
            }

            if (distance < minDist){
              minDist = distance;
              nearestCentroid[wordId] = centroidIndex;
            }
          }
          relationCounts[nearestCentroid[wordId]]++;
          for (int j = 0; j < vectorSize; j++){
            kmCentroidsNew[nearestCentroid[wordId]][j] += dataBigVector[j];
          }
        }
        SPI_finish();
      }
      // calculate new km-centroids
      for (int cs = 0; cs < k; cs++){
        for (int pos = 0; pos < vectorSize; pos++){
          if (relationCounts[cs] > 0){
            kmCentroids[cs][pos] = kmCentroidsNew[cs][pos] / relationCounts[cs];
          }else{
            kmCentroids[cs][pos] = 0;
          }
          kmCentroidsNew[cs][pos] = kmCentroids[cs][pos];
        }
      }
    }

    usrfctx = (UsrFctxCluster*) palloc (sizeof (UsrFctxCluster));
    usrfctx -> ids = inputIds;
    usrfctx -> size = inputIdsSize;
    usrfctx -> nearestCentroid = nearestCentroid;
    usrfctx -> centroids = kmCentroidsNew;
    usrfctx -> iter = 0;
    usrfctx -> k = k;

    usrfctx -> values = (char **) palloc (2 * sizeof (char *));
    usrfctx -> values  [0] = (char*) palloc   ((18 * vectorSize + 4) * sizeof (char));
    usrfctx -> values  [1] = (char*) palloc  ((inputIdsSize * 8) * sizeof (char));
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (2 , false);
    TupleDescInitEntry (outtertupdesc,  1, "Vector",    FLOAT4ARRAYOID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "Ids",INT4ARRAYOID,  -1, 0);
    slot = TupleDescGetSlot (outtertupdesc);
    funcctx -> slot = slot;
    attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
    funcctx -> attinmeta = attinmeta;
    MemoryContextSwitchTo (oldcontext);

  }
  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctxCluster*) funcctx -> user_fctx;

 // return results
  if (usrfctx->iter >= usrfctx->k){
    SRF_RETURN_DONE (funcctx);
  }else{

    Datum result;
    HeapTuple outTuple;
    char* cursor;

    // construct output values[0] -> cluster vector; values[1] -> id array
    sprintf(usrfctx->values[0], "{ ");
    cursor = usrfctx->values[0] + strlen("{ ");
    for (int i = 0; i < vectorSize; i++){
      if (i < vectorSize - 1 ){
        cursor += sprintf(cursor, "%f, ", usrfctx->centroids[usrfctx->iter][i]);
      }else{
        sprintf(cursor, "%f}", usrfctx->centroids[usrfctx->iter][i]);
      }
    }

    sprintf(usrfctx->values[1], "{ ");
    cursor = usrfctx->values[1] + strlen("{ ");
    for (int i = 0; i < usrfctx->size; i++){
      if (usrfctx->nearestCentroid[usrfctx->ids[i]] == usrfctx->iter){
      cursor += sprintf(cursor, " %d,", usrfctx->ids[i]);
      }
    }
    sprintf(cursor-1, "}");

    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
              usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }
}

PG_FUNCTION_INFO_V1(grouping_pq);

Datum
grouping_pq(PG_FUNCTION_ARGS)
{

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctxGrouping *usrfctx;
  int vectorSize = 300;

  if (SRF_IS_FIRSTCALL ()){

    int n = 0;

    MemoryContext  oldcontext;

    Datum* idsData;
    Datum* groupIdData;

    int* groupIds;
    int groupIdsSize;

    int* inputIds;
    int inputIdsSize;

    float** groupVecs;
    int groupVecsSize;

    float** querySimilarities;

    Codebook cb;
    int cbPositions = 0;
    int cbCodes = 0;
    int subVectorSize;

    int* nearestGroup;

    int ret;
    int proc;
    bool info;
    char* command;
    char * cur;

    char* tableName = palloc(sizeof(char)*100);
    char* tableNameCodebook = palloc(sizeof(char)*100);
    char* tableNamePqQuantization = palloc(sizeof(char)*100);

    getTableName(NORMALIZED, tableName, 100);
    getTableName(CODEBOOK, tableNameCodebook, 100);
    getTableName(PQ_QUANTIZATION, tableNamePqQuantization, 100);

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    // read ids from function args

    getArray(PG_GETARG_ARRAYTYPE_P(0), &idsData, &n);
    inputIds = palloc(n*sizeof(int));
    for (int j=0; j< n; j++){
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdsSize = n;

    // read group ids from function args
    getArray(PG_GETARG_ARRAYTYPE_P(1), &groupIdData, &n);
    groupIds = palloc(n*sizeof(int));
    for (int j=0; j< n; j++){
      groupIds[j] = DatumGetInt32(groupIdData[j]);
    }
    groupIdsSize = n;
    qsort(groupIds, groupIdsSize, sizeof(int), compare);

    // read group vectors
    SPI_connect();
    command = palloc( 100* sizeof(char) + inputIdsSize*10*sizeof(char));
    sprintf(command, "SELECT id, vector FROM %s WHERE id IN (", tableName);
    // fill command
    cur = command + strlen(command);
    for (int i = 0; i < groupIdsSize; i++){
      if ( i == groupIdsSize - 1){
          cur += sprintf(cur, "%d", groupIds[i]);
      }else{
        cur += sprintf(cur, "%d, ", groupIds[i]);
      }
    }
    cur += sprintf(cur, ") ORDER BY id ASC");

    groupVecs = SPI_palloc(sizeof(float*)*n);
    groupVecsSize = n;

    ret = SPI_exec(command, 0);
    proc = SPI_processed;
    if (proc != groupIdsSize){
      elog(ERROR, "Group ids do not exist");
    }
    if (ret > 0 && SPI_tuptable != NULL){
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;
      for (int i = 0; i < proc; i++){
        Datum groupVector;
        float4* dataGroupVector;
        HeapTuple tuple = tuptable->vals[i];

        groupVector = SPI_getbinval(tuple, tupdesc, 2, &info);
        n = 0;
        convert_bytea_float4(DatumGetByteaP(groupVector), &dataGroupVector, &n);
        vectorSize = n; // one asignment would be enough...
        groupVecs[i] = SPI_palloc(sizeof(float)*vectorSize);
        for (int j = 0; j < vectorSize; j++){
          groupVecs[i][j] = dataGroupVector[j];
        }
      }
    }
    SPI_finish();

    nearestGroup = palloc(sizeof(int)*(inputIdsSize));

    // get pq codebook
    cb = getCodebook(&cbPositions, &cbCodes, tableNameCodebook);

    subVectorSize = vectorSize / cbPositions;

    querySimilarities = palloc(sizeof(float*) * groupVecsSize);

    for (int cs = 0; cs < groupVecsSize; cs++){
      querySimilarities[cs] = palloc(cbPositions*cbCodes*sizeof(float));
      getPrecomputedDistances(querySimilarities[cs], cbPositions, cbCodes, subVectorSize, groupVecs[cs], cb);
    }
    // get vectors for group_ids
    // get codes for all entries with an id in inputIds -> SQL Query
    SPI_connect();
    command = palloc(200* sizeof(char) + inputIdsSize*8*sizeof(char));
    sprintf(command, "SELECT pq_quantization.id, pq_quantization.vector FROM %s AS pq_quantization WHERE pq_quantization.id IN (", tableNamePqQuantization);
    // fill command
    cur = command + strlen(command);
    for (int i = 0; i < inputIdsSize; i++){
      if ( i == inputIdsSize - 1){
          cur += sprintf(cur, "%d", inputIds[i]);
      }else{
        cur += sprintf(cur, "%d, ", inputIds[i]);
      }
    }
    cur += sprintf(cur, ")");

    ret = SPI_exec(command, 0);
    proc = SPI_processed;
    inputIdsSize = proc;
    if (ret > 0 && SPI_tuptable != NULL){
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;
      int i;
      for (i = 0; i < proc; i++){
        Datum id;
        Datum pqVector;

        int16* dataPqVector;

        float distance;
        int pqSize;

        // variables to determine best match
        float minDist = 100; // sufficient high value

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        pqVector = SPI_getbinval(tuple, tupdesc, 2, &info);

        inputIds[i] = DatumGetInt32(id);
        n = 0;
        convert_bytea_int16(DatumGetByteaP(pqVector), &dataPqVector, &n);
        pqSize = n;


        for (int groupIndex = 0; groupIndex < groupVecsSize; groupIndex++){
          distance = 0;
          for (int j = 0; j < pqSize; j++){
            int code = dataPqVector[j];
            distance += querySimilarities[groupIndex][j*cbCodes + code];
          }

          if (distance < minDist){
            minDist = distance;
            nearestGroup[i] = groupIndex;

          }
        }
      }
      SPI_finish();
    }

    usrfctx = (UsrFctxGrouping*) palloc (sizeof (UsrFctxGrouping));
    usrfctx -> ids = inputIds;
    usrfctx -> size = inputIdsSize;
    usrfctx -> nearestGroup = nearestGroup;
    usrfctx -> groups = groupIds;
    usrfctx -> iter = 0;
    usrfctx -> groupsSize = groupIdsSize;
    usrfctx -> values = (char **) palloc (2 * sizeof (char *));
    usrfctx -> values  [0] = (char*) palloc  ((18) * sizeof (char));
    usrfctx -> values  [1] = (char*) palloc   ((18 * vectorSize + 4) * sizeof (char));
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (2 , false);
    TupleDescInitEntry (outtertupdesc,  1, "Ids",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "GroupIds",INT4OID,  -1, 0);
    slot = TupleDescGetSlot (outtertupdesc);
    funcctx -> slot = slot;
    attinmeta = TupleDescGetAttInMetadata (outtertupdesc);
    funcctx -> attinmeta = attinmeta;

    MemoryContextSwitchTo (oldcontext);

  }
  funcctx = SRF_PERCALL_SETUP ();
  usrfctx = (UsrFctxGrouping*) funcctx -> user_fctx;

 // return results
  if (usrfctx->iter >= usrfctx->size){
    SRF_RETURN_DONE (funcctx);
  }else{

    Datum result;
    HeapTuple outTuple;

    sprintf(usrfctx->values[0], "%d", usrfctx->ids[usrfctx->iter]);
    sprintf(usrfctx->values[1], "%d", usrfctx -> groups[usrfctx->nearestGroup[usrfctx->iter]]);

    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
              usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }
}


PG_FUNCTION_INFO_V1(insert_batch);

Datum
insert_batch(PG_FUNCTION_ARGS)
{

  int n;

  Datum* termsData;
  int inputTermsSize;
  int inputTermsPlaneSize;
  char** inputTerms;

  int ret;
  int proc;
  bool info;
  char* command;
  char* cur;

  float4** rawVectors;
  float4** rawVectorsUnnormalized;
  int vectorSize = 0;
  char** tokens;
  int rawVectorsSize;

  CodebookWithCounts cb;
  int cbPositions = 0;
  int cbCodes = 0;
  int subvectorSize;

  int** nearestCentroids;
  int* countIncs;

  int* cqQuantizations;
  float* nearestCoarseCentroidRaw;
  float** residuals;

  CodebookWithCounts residualCb;
  int cbrPositions = 0;
  int cbrCodes = 0;
  int residualSubvectorSize;
  CoarseQuantizer cq;
  int cqSize;
  int** nearestResidualCentroids;
  int* residualCountIncs;
  float minDistCoarse;


  char* tableNameCodebook = palloc(sizeof(char)*100);
  char* pqQuantizationTable = palloc(sizeof(char)*100);
  char* tableNameResidualCodebook = palloc(sizeof(char)*100);
  char* tableNameFineQuantization = palloc(sizeof(char)*100);

  char* tableNameNormalized = palloc(sizeof(char)*100);
  char* tableNameOriginal = palloc(sizeof(char)*100);

  getTableName(CODEBOOK, tableNameCodebook, 100);
  getTableName(PQ_QUANTIZATION, pqQuantizationTable, 100);

  getTableName(RESIDUAL_CODBOOK, tableNameResidualCodebook, 100);
  getTableName(RESIDUAL_QUANTIZATION, tableNameFineQuantization, 100);

  getTableName(NORMALIZED, tableNameNormalized, 100);
  getTableName(ORIGINAL, tableNameOriginal, 100);

  // get terms from arguments
  getArray(PG_GETARG_ARRAYTYPE_P(0), &termsData, &n);
  inputTerms = palloc(n*sizeof(char*));
  inputTermsPlaneSize = 0;
  for (int j=0; j< n; j++){
    char* term = palloc(sizeof(char)*(VARSIZE(termsData[j]) - VARHDRSZ+1));
    snprintf(term, VARSIZE(termsData[j]) + 1 - VARHDRSZ, "%s",(char*) VARDATA(termsData[j]));
    inputTermsPlaneSize += strlen(term);
    inputTerms[j] = term;
  }
  inputTermsSize = n;

  // determine tokenization
  command = palloc(sizeof(char)*inputTermsPlaneSize*3 + 200);
  cur = command;
  cur += sprintf(cur, "SELECT replace(term, ' ', '_') AS token, tokenize(term), tokenize_raw(term) FROM unnest('{");
  for (int i=0; i < inputTermsSize; i++){
    if (i < (inputTermsSize-1)){
      cur += sprintf(cur, "%s, ", inputTerms[i]);
    }else{
      cur += sprintf(cur, "%s", inputTerms[i]);;
    }
  }
  cur += sprintf(cur, "}'::varchar(100)[]) AS term WHERE NOT replace(term, ' ', '_') IN (SELECT word FROM %s)", tableNameNormalized);
  SPI_connect();
  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  rawVectorsSize = proc;
  rawVectors = SPI_palloc(sizeof(float4*)*proc);
  rawVectorsUnnormalized = SPI_palloc(sizeof(float4*)*proc);
  tokens = SPI_palloc(sizeof(char*)*proc);
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    for (int i = 0; i < proc; i++){
      Datum vector;
      Datum vectorUnnormalized;
      char* token;

      float4* tmpVector;

      HeapTuple tuple = tuptable->vals[i];

      token = SPI_getvalue(tuple, tupdesc, 1);
      vector = SPI_getbinval(tuple, tupdesc, 2, &info);
      vectorUnnormalized = SPI_getbinval(tuple, tupdesc, 3, &info);
      tokens[i] = SPI_palloc(sizeof(char)* 100); // maybe replace with strlen(token)+1 when using TEXT data type

      snprintf(tokens[i], strlen(token)+1, "%s", token);
      n = 0;
      convert_bytea_float4(DatumGetByteaP(vector), &tmpVector, &n);
      rawVectors[i] = SPI_palloc(sizeof(float4)*n);
      memcpy(rawVectors[i], tmpVector, n*sizeof(float4));
      n = 0;
      convert_bytea_float4(DatumGetByteaP(vectorUnnormalized), &tmpVector, &n);
      rawVectorsUnnormalized[i] = SPI_palloc(sizeof(float4)*n);
      memcpy(rawVectorsUnnormalized[i], tmpVector, n*sizeof(float4));
      vectorSize = n;
    }
    SPI_finish();
  }
  // determine quantization and count increments
  // get pq codebook
  cb = getCodebookWithCounts(&cbPositions, &cbCodes, tableNameCodebook);
  // get residual codebook
  residualCb = getCodebookWithCounts(&cbrPositions, &cbrCodes, tableNameResidualCodebook);
  // get coarse quantizer
  cq = getCoarseQuantizer(&cqSize);
  // determine coarse quantization and residuals
  cqQuantizations = palloc(sizeof(int)*rawVectorsSize);
  nearestCoarseCentroidRaw = NULL;
  residuals = palloc(sizeof(float*)*rawVectorsSize);

  for (int i = 0; i < rawVectorsSize; i++){
    minDistCoarse = 100;
    for (int j = 0; j < cqSize; j++){
      float dist = squareDistance(rawVectors[i], cq[j].vector, vectorSize);
      if (dist < minDistCoarse){
        cqQuantizations[i] = cq[j].id;
        nearestCoarseCentroidRaw = cq[j].vector;
        minDistCoarse = dist;
      }
    }
    residuals[i] = palloc(sizeof(float)*vectorSize);
    for (int j = 0; j < vectorSize; j++){
      residuals[i][j] = rawVectors[i][j] - nearestCoarseCentroidRaw[j];
    }
  }
  // determine nearest centroids (quantization)
  nearestCentroids = palloc(sizeof(int*)*rawVectorsSize);
  countIncs = palloc(cbPositions*cbCodes*sizeof(int));
  subvectorSize = vectorSize / cbPositions;

  updateCodebook(rawVectors, rawVectorsSize, subvectorSize, cb, cbPositions, cbCodes, nearestCentroids, countIncs);

  nearestResidualCentroids = palloc(sizeof(int*)*rawVectorsSize);
  residualSubvectorSize = vectorSize / cbrPositions;
  residualCountIncs = palloc(cbrPositions*cbrCodes*sizeof(int));
  updateCodebook(residuals, rawVectorsSize, residualSubvectorSize, residualCb, cbrPositions, cbrCodes, nearestResidualCentroids, residualCountIncs);

  // insert new terms + quantinzation
  updateProductQuantizationRelation(nearestCentroids, tokens, cbPositions, cb, pqQuantizationTable, rawVectorsSize, NULL);
  // insert new terms + quantinzation for residuals
  updateProductQuantizationRelation(nearestResidualCentroids, tokens, cbrPositions, residualCb, tableNameFineQuantization, rawVectorsSize, cqQuantizations);
  // update codebook relation
  updateCodebookRelation(cb, cbPositions, cbCodes, tableNameCodebook, countIncs, subvectorSize);
  // update residual codebook relation
  updateCodebookRelation(residualCb, cbrPositions, cbrCodes, tableNameResidualCodebook, residualCountIncs, residualSubvectorSize);

  updateWordVectorsRelation(tableNameNormalized, tokens, rawVectors, rawVectorsSize, vectorSize);
  updateWordVectorsRelation(tableNameOriginal, tokens, rawVectorsUnnormalized, rawVectorsSize, vectorSize);

  PG_RETURN_INT32(0);

}

PG_FUNCTION_INFO_V1(read_bytea);

Datum
read_bytea(PG_FUNCTION_ARGS)
{

  typedef struct{
      int16       elmlen;
      bool        elmbyval;
      char        elmalign;
      Oid         elmtype;
  } elm_info; // TODO define this in some header file

  bytea *data = PG_GETARG_BYTEA_P(0);
  int32* out;

  int size = 0;

  ArrayType* v;
  Datum* dvalues;
  int dims[1];
  int lbs[1];
  elm_info info;

  convert_bytea_int32(data, &out, &size);

  dvalues = (Datum*) palloc(sizeof(Datum)*size);
  for (int i = 0; i < size; i++){
    dvalues[i] = Int32GetDatum(out[i]);
  }

  dims[0] = size;
  lbs[0] = 1;

  info.elmtype = INT4OID;
  get_typlenbyvalalign(info.elmtype, &info.elmlen, &info.elmbyval,  &info.elmalign);

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, info.elmtype, info.elmlen, info.elmbyval, info.elmalign);

  PG_RETURN_ARRAYTYPE_P(v);

}

PG_FUNCTION_INFO_V1(read_bytea_int16);

Datum
read_bytea_int16(PG_FUNCTION_ARGS)
{

  typedef struct{
      int16       elmlen;
      bool        elmbyval;
      char        elmalign;
      Oid         elmtype;
  } elm_info; // TODO define this in some header file

  bytea *data = PG_GETARG_BYTEA_P(0);
  int16* out;

  int size = 0;

  ArrayType* v;
  Datum* dvalues;
  int dims[1];
  int lbs[1];
  elm_info info;

  convert_bytea_int16(data, &out, &size);

  dvalues = (Datum*) palloc(sizeof(Datum)*size);
  for (int i = 0; i < size; i++){
    dvalues[i] = Int16GetDatum(out[i]);
  }

  dims[0] = size;
  lbs[0] = 1;

  info.elmtype = INT2OID;
  get_typlenbyvalalign(info.elmtype, &info.elmlen, &info.elmbyval,  &info.elmalign);

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, info.elmtype, info.elmlen, info.elmbyval, info.elmalign);

  PG_RETURN_ARRAYTYPE_P(v);

}

PG_FUNCTION_INFO_V1(read_bytea_float);

Datum
read_bytea_float(PG_FUNCTION_ARGS)
{

  typedef struct{
      int16       elmlen;
      bool        elmbyval;
      char        elmalign;
      Oid         elmtype;
  } elm_info; // TODO define this in some header file

  bytea *data = PG_GETARG_BYTEA_P(0);
  float4* out;

  int size = 0;

  ArrayType* v;
  Datum* dvalues;
  int dims[1];
  int lbs[1];
  elm_info info;

  convert_bytea_float4(data, &out, &size);

  dvalues = (Datum*) palloc(sizeof(Datum)*size);
  for (int i = 0; i < size; i++){
    dvalues[i] = Float4GetDatum(out[i]);
  }

  dims[0] = size;
  lbs[0] = 1;

  info.elmtype = FLOAT4OID;
  get_typlenbyvalalign(info.elmtype, &info.elmlen, &info.elmbyval,  &info.elmalign);

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, info.elmtype, info.elmlen, info.elmbyval, info.elmalign);

  PG_RETURN_ARRAYTYPE_P(v);

}

PG_FUNCTION_INFO_V1(vec_to_bytea);

Datum
vec_to_bytea(PG_FUNCTION_ARGS)
{

  bytea * out;

  Datum* vectorData;
  int n = 0;
  ArrayType* array = PG_GETARG_ARRAYTYPE_P(0);
  Oid eltype = ARR_ELEMTYPE(array);

  getArray(array, &vectorData, &n);

  switch(eltype) {
    case FLOAT4OID:
    {
      float4* vector;
      vector = palloc(n*sizeof(float4));
      for (int j=0; j< n; j++){
        vector[j] = DatumGetFloat4(vectorData[j]);
      }
      convert_float4_bytea(vector, &out, n);
    }
    break;
    case INT4OID:
    {
      int32* vector = palloc(n*sizeof(int32));;
      for (int j=0; j< n; j++){
        vector[j] = DatumGetInt32(vectorData[j]);
      }
      convert_int32_bytea(vector, &out, n);
    }
    break;
    case INT2OID:
    {
      int16* vector = palloc(n*sizeof(int16));;
      for (int j=0; j< n; j++){
        vector[j] = DatumGetInt16(vectorData[j]);
      }
      convert_int16_bytea(vector, &out, n);
      break;
    }
    default:
      elog(ERROR, "Unknown element type: %d", (int) eltype);
  }

  PG_RETURN_BYTEA_P(out);

}
