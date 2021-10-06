
// #define LOG_TARGET_COUNTS

// clang-format off

#include "index_utils.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "stdlib.h"
#include "time.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/array.h"

// clang-format on

void updateTopK(TopK tk, float distance, int id, int k, int maxDist) {
  int i;
  for (i = k - 1; i >= 0; i--) {
    if (tk[i].distance < distance) {
      break;
    }
  }
  i++;
  for (int j = k - 2; j >= i; j--) {
    tk[j + 1].distance = tk[j].distance;
    tk[j + 1].id = tk[j].id;
  }
  tk[i].distance = distance;
  tk[i].id = id;
}

void updateTopKPV(TopKPV tk, float distance, int id, int k, int maxDist,
                  float4* vector, int dim) {
  int i;
  for (i = k - 1; i >= 0; i--) {
    if (tk[i].distance < distance) {
      break;
    }
  }
  i++;
  for (int j = k - 2; j >= i; j--) {
    tk[j + 1].distance = tk[j].distance;
    tk[j + 1].id = tk[j].id;
    tk[j + 1].vector = tk[j].vector;
  }
  tk[i].distance = distance;
  tk[i].id = id;
  tk[i].vector = vector;
}

void updateTopKWordEntry(char** term, char* word) {
  char* cur = word;
  memset(word, 0, strlen(word));
  for (int p = 0; term[p]; p++) {
    if (term[p + 1] == NULL) {
      cur += sprintf(cur, "%s", term[p]);
    } else {
      cur += sprintf(cur, "%s ", term[p]);
    }
  }
}

void initTopK(TopK* pTopK, int k, const float maxDist) {
  *pTopK = palloc(k * sizeof(TopKEntry));
  for (int i = 0; i < k; i++) {
    (*pTopK)[i].distance = maxDist;
    (*pTopK)[i].id = -1;
  }
}

void initTopKs(TopK** pTopKs, float** pMaxDists, int queryVectorsSize, int k,
               const float maxDist) {
  *pTopKs = palloc(queryVectorsSize * sizeof(TopK));
  *pMaxDists = palloc(sizeof(float) * queryVectorsSize);
  for (int i = 0; i < queryVectorsSize; i++) {
    initTopK(&((*pTopKs)[i]), k, maxDist);
    (*pMaxDists)[i] = maxDist;
  }
}

void initTopKPV(TopKPV* pTopK, int k, const float maxDist, int dim) {
  *pTopK = palloc(k * sizeof(TopKPVEntry));
  for (int i = 0; i < k; i++) {
    (*pTopK)[i].distance = maxDist;
    (*pTopK)[i].id = -1;
    // (*pTopK)[i].vector = palloc(sizeof(float4)*dim);
    (*pTopK)[i].vector = NULL;
  }
}

void initTopKPVs(TopKPV** pTopKs, float** pMaxDists, int queryVectorsSize,
                 int k, const float maxDist, int dim) {
  *pTopKs = palloc(queryVectorsSize * sizeof(TopKPV));
  *pMaxDists = palloc(sizeof(float) * queryVectorsSize);
  for (int i = 0; i < queryVectorsSize; i++) {
    initTopKPV(&((*pTopKs)[i]), k, maxDist, dim);
    (*pMaxDists)[i] = maxDist;
  }
}

int cmpTopKPVEntry(const void* a, const void* b) {
  return ((((float)(*(TopKPVEntry*)a).distance) >
           ((float)(*(TopKPVEntry*)b).distance)) -
          ((float)((*(TopKPVEntry*)a).distance) <
           ((float)(*(TopKPVEntry*)b).distance)));
}

int cmpTopKEntry(const void* a, const void* b) {
  return ((((float)(*(TopKEntry*)a).distance) >
           ((float)(*(TopKEntry*)b).distance)) -
          ((float)((*(TopKEntry*)a).distance) <
           ((float)(*(TopKEntry*)b).distance)));
}

void push(DistancePQueue* queue, float distance, int id, int positions[2]) {
  int i = queue->len;
  int j = (i - 1) / 2;
  while ((i > 0) && (queue->nodes[j].distance > distance)) {
    queue->nodes[i] = queue->nodes[j];
    i = j;
    j = (j - 1) / 2;
  }
  queue->nodes[i].distance = distance;
  queue->nodes[i].id = id;
  queue->nodes[i].positions[0] = positions[0];
  queue->nodes[i].positions[1] = positions[1];
  queue->len++;
}

QueueEntry pop(DistancePQueue* queue) {
  QueueEntry result = queue->nodes[0];
  int j;
  int last;
  int i = 0;
  queue->nodes[0] = queue->nodes[queue->len - 1];
  queue->len--;
  while (i != queue->len) {
    last = queue->len;
    j = 1 + (i * 2);
    if ((j <= (queue->len - 1)) &&
        (queue->nodes[j].distance < queue->nodes[last].distance)) {
      last = j;
    }
    if ((j <= (queue->len - 1)) &&
        (queue->nodes[j + 1].distance < queue->nodes[last].distance)) {
      last = j + 1;
    }
    queue->nodes[i] = queue->nodes[last];
    i = last;
  }
  return result;
}

bool inBlacklist(int id, Blacklist* bl) {
  if (bl->isValid) {
    if (bl->id == id) {
      return true;
    } else {
      return inBlacklist(id, bl->next);
    }
  } else {
    return false;
  }
}

void addToBlacklist(int id, Blacklist* bl, Blacklist* emptyBl) {
  while (bl->isValid) {
    bl = bl->next;
  }
  bl->id = id;
  bl->next = emptyBl;
  bl->isValid = true;
}

bool determineCoarseIdsMultiWithStatistics(
    int*** pCqIds, int*** pCqTableIds, int** pCqTableIdCounts,
    int* queryVectorsIndices, int queryVectorsIndicesSize, int queryVectorsSize,
    float maxDist, CoarseQuantizer cq, int cqSize, float4** queryVectors,
    int queryDim, float* statistics, int inputIdsSize, const int minTargetCount,
    const float confidence) {
  int** cqIds;
  int** cqTableIds;
  int* cqTableIdCounts;
  TopK minDist;
  float dist;
  float targetCount;
  bool lastIteration = true;

  minDist = palloc(sizeof(TopKEntry) * cqSize);

  *pCqIds = palloc(queryVectorsSize * sizeof(int*));
  cqIds = *pCqIds;

  *pCqTableIds = palloc(sizeof(int*) * cqSize);
  cqTableIds = *pCqTableIds;

  *pCqTableIdCounts = palloc(sizeof(int) * cqSize);
  cqTableIdCounts = *pCqTableIdCounts;

  for (int i = 0; i < cqSize; i++) {
    cqTableIds[i] = NULL;
    cqTableIdCounts[i] = 0;
  }

  for (int x = 0; x < queryVectorsIndicesSize; x++) {
    int queryIndex = queryVectorsIndices[x];
    int max_coarse_order = 0;
    float prob = 0.0;
    for (int i = 0; i < cqSize; i++) {
      minDist[i].id = -1;
      minDist[i].distance = maxDist;
    }

    for (int j = 0; j < cqSize; j++) {
      dist = squareDistance(queryVectors[queryIndex], cq[j].vector, queryDim);
      minDist[j].id = j;
      minDist[j].distance = dist;
    }
    qsort(minDist, cqSize, sizeof(TopKEntry), cmpTopKEntry);
    targetCount = 0;
    while ((getConfidenceHyp(minTargetCount, inputIdsSize, prob,
                             statistics[cqSize]) < confidence) &&
           (max_coarse_order < cqSize)) {
      targetCount += statistics[minDist[max_coarse_order].id] * inputIdsSize;
      prob += statistics[minDist[max_coarse_order].id];
      max_coarse_order++;
    }
    if (max_coarse_order < cqSize) {
      lastIteration = false;
    }
    // elog(INFO, "TRACK target_count %f", targetCount); // to get statistics
    // about the quality of the prediction
    cqIds[queryIndex] = palloc(sizeof(int) * max_coarse_order);
    for (int i = 0; i < max_coarse_order; i++) {
      int cqId = minDist[i].id;
      cqIds[queryIndex][i] = cqId;

      if (cqTableIdCounts[cqId] == 0) {
        cqTableIds[cqId] = palloc(sizeof(int) * queryVectorsIndicesSize);
      }
      cqTableIds[cqId][cqTableIdCounts[cqId]] = queryIndex;
      cqTableIdCounts[cqId] += 1;
    }
  }

  return lastIteration;
}

bool determineCoarseIdsMultiWithStatisticsMulti(
    int*** pCqIds, int*** pCqTableIds, int** pCqTableIdCounts,
    int* queryVectorsIndices, int queryVectorsIndicesSize, int queryVectorsSize,
    float maxDist, Codebook cq, int cqSize, int cqPositions, int cqCodes,
    float4** queryVectors, int queryDim, float* statistics, int inputIdsSize,
    const int minTargetCount, float confidence) {
  int** cqIds;
  int** cqTableIds;
  int* cqTableIdCounts;
  TopK* minDists;
  TopK minDistAll;
  float dist;
  bool lastIteration = true;
  int* powers;
  int subdim = queryDim / cqPositions;
  const bool USE_PROPERTY_QUEUE = true;
  int* currentCqIds = palloc(sizeof(int) * cqSize);
  minDists = palloc(sizeof(TopK) * cqPositions);
  for (int i = 0; i < cqPositions; i++) {
    minDists[i] = palloc(sizeof(TopKEntry) * cqCodes);
  }
  minDistAll = palloc(sizeof(TopKEntry) * cqSize);

  powers = palloc(sizeof(int) * (cqPositions + 1));
  for (int i = 0; i < cqPositions + 1; i++) {
    powers[i] = pow(cqCodes, i);
  }

  *pCqIds = palloc(queryVectorsSize * sizeof(int*));
  cqIds = *pCqIds;

  *pCqTableIds = palloc(sizeof(int*) * cqSize);
  cqTableIds = *pCqTableIds;

  *pCqTableIdCounts = palloc(sizeof(int) * cqSize);
  cqTableIdCounts = *pCqTableIdCounts;

  for (int i = 0; i < cqSize; i++) {
    cqTableIds[i] = NULL;
    cqTableIdCounts[i] = 0;
  }
  for (int x = 0; x < queryVectorsIndicesSize; x++) {
    int queryIndex = queryVectorsIndices[x];
    int max_coarse_order = 0;
    float prob = 0.0;

    for (int pos = 0; pos < cqPositions; pos++) {
      for (int j = 0; j < cqCodes; j++) {
        dist = squareDistance(queryVectors[queryIndex] + (pos * subdim),
                              cq[j + pos * cqCodes].vector, subdim);
        minDists[pos][j].id = j;
        minDists[pos][j].distance = dist;
      }
    }
    for (int i = 0; i < cqSize; i++) {
      minDistAll[i].id = i;
      minDistAll[i].distance = 0;
      for (int pos = 0; pos < cqPositions; pos++) {
        minDistAll[i].distance +=
            minDists[pos][i % powers[pos + 1] / powers[pos]].distance;
      }
    }
    if (!USE_PROPERTY_QUEUE) {
      qsort(minDistAll, cqSize, sizeof(TopKEntry), cmpTopKEntry);
    } else {
      for (int pos = 0; pos < cqPositions; pos++) {
        qsort(minDists[pos], cqCodes, sizeof(TopKEntry), cmpTopKEntry);
      }
    }

    if (USE_PROPERTY_QUEUE) {
      // ONLY IMPLEMENTED FOR cbPositions == 2!

      // create data structures
      DistancePQueue
          queue;  // priority queue storing the centroids to be traversed next
      int elemIndex;

      // bit vector matrix to store information about which centroids are
      // traversed
      u_int32_t* traversed = palloc(sizeof(uint32_t) * ((cqSize / 32) + 1));
      u_int32_t* in_queue = palloc(sizeof(uint32_t) * ((cqSize / 32) + 1));
      for (int i = 0; i < cqSize; i++) {
        traversed[i / 32] &= 0 << (i % 32);
      }
      for (int i = 0; i < cqSize; i++) {
        in_queue[i / 32] &= 0 << (i % 32);
      }

      queue.nodes = palloc(sizeof(QueueEntry) * cqSize);

      // init pqueue with element with the lowest distance value
      elemIndex = powers[0] * minDists[0][0].id + powers[1] * minDists[1][0].id;

      queue.nodes[0].positions[0] = 0;
      queue.nodes[0].positions[1] = 0;
      queue.nodes[0].id = minDistAll[elemIndex].id;
      queue.nodes[0].distance = minDistAll[elemIndex].distance;
      queue.len = 1;
      while ((getConfidenceHyp(minTargetCount, inputIdsSize, prob,
                               statistics[cqSize]) < confidence) &&
             (max_coarse_order < cqSize)) {
        QueueEntry next = pop(&queue);
        int lastElemIndex;
        int newPositionIndex = next.positions[0] + cqCodes * next.positions[1];
        traversed[newPositionIndex / 32] |= 1 << (newPositionIndex % 32);
        // add neighbors to priority queue
        lastElemIndex =
            next.positions[0] + 1 + cqCodes * (next.positions[1] - 1);
        if ((next.positions[0] < (cqCodes - 1)) &&
            ((next.positions[1] == 0) ||
             (traversed[lastElemIndex / 32] & (1 << (lastElemIndex % 32))))) {
          int nextPositionIndex;
          int newPositions[2];
          newPositions[0] = next.positions[0] + 1;
          newPositions[1] = next.positions[1];
          nextPositionIndex = newPositions[0] + cqCodes * newPositions[1];
          if (!(in_queue[nextPositionIndex / 32] &
                (1 << (nextPositionIndex % 32)))) {
            int newId = minDists[0][newPositions[0]].id +
                        cqCodes * minDists[1][newPositions[1]].id;
            push(&queue, minDistAll[newId].distance, newId, newPositions);
            in_queue[nextPositionIndex / 32] |= 1 << (nextPositionIndex % 32);
          }
        }
        lastElemIndex =
            next.positions[0] - 1 + cqCodes * (next.positions[1] + 1);

        if ((next.positions[1] < (cqCodes - 1)) &&
            ((next.positions[0] == 0) ||
             (traversed[lastElemIndex / 32] & (1 << (lastElemIndex % 32))))) {
          int nextPositionIndex;
          int newPositions[2];
          newPositions[0] = next.positions[0];
          newPositions[1] = next.positions[1] + 1;
          nextPositionIndex = newPositions[0] + cqCodes * newPositions[1];
          if (!(in_queue[nextPositionIndex / 32] &
                (1 << (nextPositionIndex % 32)))) {
            int newId = minDists[0][newPositions[0]].id +
                        cqCodes * minDists[1][newPositions[1]].id;
            push(&queue, minDistAll[newId].distance, newId, newPositions);
            in_queue[nextPositionIndex / 32] |= 1 << (nextPositionIndex % 32);
          }
        }
        // add centroids and guess number of targets
        prob += statistics[next.id];

        currentCqIds[max_coarse_order] = next.id;
        max_coarse_order++;
        if (cqTableIdCounts[next.id] == 0) {
          cqTableIds[next.id] = palloc(sizeof(int) * queryVectorsIndicesSize);
        }
        cqTableIds[next.id][cqTableIdCounts[next.id]] = queryIndex;
        cqTableIdCounts[next.id] += 1;
      }
      if (max_coarse_order < cqSize) {
        lastIteration = false;
      }
      cqIds[queryIndex] = palloc(sizeof(int) * max_coarse_order);
      memcpy(cqIds[queryIndex], currentCqIds, max_coarse_order * sizeof(int));
#ifdef LOG_TARGET_COUNTS
      elog(INFO, "TRACK target_count %f", prob * inputIdsSize);  // to get
#endif
      // statistics
    } else {
      while ((getConfidenceHyp(minTargetCount, inputIdsSize, prob,
                               statistics[cqSize]) < confidence) &&
             (max_coarse_order < (cqSize - 1))) {
        prob += statistics[minDistAll[max_coarse_order].id];
        max_coarse_order++;
      }
      if (max_coarse_order < (cqSize - 1)) {
        lastIteration = false;
      }
      // elog(INFO, "TRACK target_count %f", prob*inputIdsSize); // to get
      // statistics
      // about the quality of the prediction
      cqIds[queryIndex] = palloc(sizeof(int) * max_coarse_order);
      for (int i = 0; i < max_coarse_order; i++) {
        int cqId = minDistAll[i].id;
        cqIds[queryIndex][i] = cqId;

        if (cqTableIdCounts[cqId] == 0) {
          cqTableIds[cqId] = palloc(sizeof(int) * queryVectorsIndicesSize);
        }
        cqTableIds[cqId][cqTableIdCounts[cqId]] = queryIndex;
        cqTableIdCounts[cqId] += 1;
      }
    }
  }
  return lastIteration;
}

void getPrecomputedDistances(float4* preDists, int cbPositions, int cbCodes,
                             int subvectorSize, float4* queryVector,
                             Codebook cb) {
  for (int i = 0; i < cbPositions * cbCodes; i++) {
    int pos = cb[i].pos;
    int code = cb[i].code;
    float* vector = cb[i].vector;
    preDists[pos * cbCodes + code] = squareDistance(
        queryVector + (pos * subvectorSize), vector, subvectorSize);
  }
}

void getPrecomputedDistancesDouble(float4* preDists, int cbPositions,
                                   int cbCodes, int subvectorSize,
                                   float4* queryVector, Codebook cb) {
  for (int i = 0; i < (cbPositions / 2); i++) {
    int pos = i * 2;
    int pointer = cbCodes * cbCodes * i;

    for (int j = 0; j < cbCodes * cbCodes; j++) {
      int p1 = (j % cbCodes) + pos * cbCodes;  // positions in cb
      int p2 = (j / cbCodes) + (pos + 1) * cbCodes;
      int code = cb[p1].code + cbCodes * cb[p2].code;
      preDists[pointer + code] =
          squareDistance(queryVector + (pos * subvectorSize), cb[p1].vector,
                         subvectorSize) +
          squareDistance(queryVector + ((pos + 1) * subvectorSize),
                         cb[p2].vector, subvectorSize);
    }
  }
}

void postverify(int* queryVectorsIndices, int queryVectorsIndicesSize, int k,
                int pvf, TopKPV* topKPVs, TopK* topKs, float4** queryVectors,
                int queryDim, const float maxDistance) {
  for (int x = 0; x < queryVectorsIndicesSize; x++) {
    int queryIndex = queryVectorsIndices[x];
    float maxDist = maxDistance;
    float distance;

    for (int j = 0; j < k * pvf; j++) {
      // calculate distances
      if (topKPVs[queryIndex][j].id != -1) {
        distance = squareDistance(queryVectors[queryIndex],
                                  topKPVs[queryIndex][j].vector, queryDim);
        if (distance < maxDist) {
          updateTopK(topKs[queryIndex], distance, topKPVs[queryIndex][j].id, k,
                     maxDist);
          maxDist = topKs[queryIndex][k - 1].distance;
        }
      }
    }
  }
}

float squareDistance(float* v1, float* v2, int n) {
  float result = 0;
  for (int i = 0; i < n; i++) {
    float prod = (v1[i] - v2[i]) * (v1[i] - v2[i]);
    result += prod;
  }

  return result;
}

void shuffle(int* input, int* output, int inputSize, int outputSize) {
  int i;
  int j;
  int t;
  int* tmp = palloc(sizeof(int) * inputSize);
  srand(time(0));

  for (i = 0; i < inputSize; i++) {
    tmp[i] = input[i];
  }
  for (i = 0; i < outputSize; i++) {
    j = i + rand() / (RAND_MAX / (inputSize - i) + 1);
    t = tmp[j];
    tmp[j] = tmp[i];
    tmp[i] = t;
  }
  for (i = 0; i < outputSize; i++) {
    output[i] = tmp[i];
  }
}

CoarseQuantizer getCoarseQuantizer(int* size) {
  char* command;
  ResultInfo rInfo;

  float4* tmp;

  CoarseQuantizer result;
  char* tableNameCQ = palloc(sizeof(char) * 100);
  getTableName(COARSE_QUANTIZATION, tableNameCQ, 100);

  SPI_connect();
  command = palloc(100 * sizeof(char));
  sprintf(command, "SELECT * FROM %s", tableNameCQ);
  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;
  *size = rInfo.proc;
  result = SPI_palloc(rInfo.proc * sizeof(CoarseQuantizerEntry));
  if (rInfo.ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < rInfo.proc; i++) {
      Datum id;
      Datum vector;

      int n = 0;

      bytea* vectorData;

      HeapTuple tuple = tuptable->vals[i];
      id = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
      vector = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);
      vectorData = DatumGetByteaP(vector);

      result[i].id = DatumGetInt32(id);
      tmp = (float4*)VARDATA(vectorData);
      result[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result[i].vector, tmp, n * sizeof(float4));
    }
    SPI_finish();
  }

  return result;
}

CodebookCompound getCodebook(char* tableName) {
  char command[100];
  ResultInfo rInfo;

  float4* tmp;

  CodebookCompound result;

  bytea* vectorData;

  result.codeSize = 0;
  result.positions = 0;

  SPI_connect();
  sprintf(command, "SELECT * FROM %s ORDER BY pos", tableName);
  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;
  result.codebook = SPI_palloc(rInfo.proc * sizeof(CodebookEntry));
  if (rInfo.ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < rInfo.proc; i++) {
      Datum pos;
      Datum code;
      Datum vector;

      int n = 0;

      HeapTuple tuple = tuptable->vals[i];
      pos = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);
      code = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
      vector = SPI_getbinval(tuple, tupdesc, 4, &rInfo.info);

      result.positions = fmax(result.positions, pos);
      result.codeSize = fmax(result.codeSize, code);

      result.codebook[i].pos = DatumGetInt32(pos);
      result.codebook[i].code = DatumGetInt32(code);

      vectorData = DatumGetByteaP(vector);
      tmp = (float4*)VARDATA(vectorData);
      result.codebook[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result.codebook[i].vector, tmp, n * sizeof(float4));
    }
    SPI_finish();
  }

  result.positions += 1;
  result.codeSize += 1;

  return result;
}

float* getStatistics() {
  float* result;
  char command[100];
  ResultInfo rInfo;
  char* tableName = palloc(sizeof(char) * 100);

  getTableName(STATISTICS, tableName, 100);

  SPI_connect();
  sprintf(command, "SELECT coarse_id, coarse_freq FROM %s", tableName);

  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;

  result = SPI_palloc(rInfo.proc * sizeof(float));

  if (rInfo.ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < rInfo.proc; i++) {
      HeapTuple tuple = tuptable->vals[i];

      int coarse_id =
          DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &rInfo.info));
      result[coarse_id] =
          DatumGetFloat4(SPI_getbinval(tuple, tupdesc, 2, &rInfo.info));
    }
  }

  SPI_finish();

  return result;
}

float getConfidenceBin(int expect, int size, float p) {
  float mu = size * p;
  float sig = sqrt(size * p * (1.0 - p));
  return 1.0 - 0.5 * (1 + erf((expect - 0.5 - mu) / (sig * sqrt(2))));
}

float getConfidenceHyp(int expect, int size, float p, int stat_size) {
  if (expect > size) {
    return 0;
  }
  float mu = size * p;
  float sig = sqrt(size * p * (1.0 - p)) *
              (((float)stat_size - size) / ((float)stat_size - 1.0));
  return 1.0 -
         0.5 * (1.0 + erf((((float)expect) - 0.5 - mu) / (sig * sqrt(2))));
}

CodebookWithCounts getCodebookWithCounts(int* positions, int* codesize,
                                         char* tableName) {
  char command[50];
  ResultInfo rInfo;
  float4* tmp;

  bytea* vectorData;

  CodebookWithCounts result;
  SPI_connect();
  sprintf(command, "SELECT * FROM %s", tableName);

  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;
  result = SPI_palloc(rInfo.proc * sizeof(CodebookEntryComplete));
  if (rInfo.ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < rInfo.proc; i++) {
      Datum pos;
      Datum code;
      Datum vector;
      Datum count;
      int n = 0;

      HeapTuple tuple = tuptable->vals[i];
      pos = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);
      code = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
      vector = SPI_getbinval(tuple, tupdesc, 4, &rInfo.info);
      count = SPI_getbinval(tuple, tupdesc, 5, &rInfo.info);

      (*positions) = fmax((*positions), pos);
      (*codesize) = fmax((*codesize), code);

      result[i].pos = DatumGetInt32(pos);
      result[i].code = DatumGetInt32(code);
      vectorData = DatumGetByteaP(vector);
      tmp = (float4*)VARDATA(vectorData);

      result[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result[i].vector, tmp, n * sizeof(float4));
      result[i].count = DatumGetInt32(count);
    }
    SPI_finish();
  }

  *positions += 1;
  *codesize += 1;

  return result;
}

WordVectors getVectors(char* tableName, int* ids, int idsSize) {
  char* command;
  char* cur;
  ResultInfo rInfo;

  int n = 0;

  WordVectors result;

  result.vectors = palloc(sizeof(float*) * idsSize);
  result.ids = palloc(sizeof(int) * idsSize);

  SPI_connect();

  command = palloc((100 + idsSize * 18) * sizeof(char));
  sprintf(command, "SELECT id, vector FROM %s WHERE id IN (", tableName);
  // fill command
  cur = command + strlen(command);
  for (int i = 0; i < idsSize; i++) {
    if (i == idsSize - 1) {
      cur += sprintf(cur, "%d", ids[i]);
    } else {
      cur += sprintf(cur, "%d, ", ids[i]);
    }
  }
  cur += sprintf(cur, ")");

  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;
  if (rInfo.ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < rInfo.proc; i++) {
      Datum id;
      Datum vector;
      float4* data;

      int wordId;

      HeapTuple tuple = tuptable->vals[i];
      id = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
      vector = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);
      wordId = DatumGetInt32(id);

      convert_bytea_float4(DatumGetByteaP(vector), &data, &n);

      result.vectors[i] = SPI_palloc(sizeof(float) * n);
      result.ids[i] = wordId;
      for (int j = 0; j < n; j++) {
        result.vectors[i][j] = data[j];
      }
    }
  }
  SPI_finish();

  return result;
}

void getArray(ArrayType* input, Datum** result, int* n) {
  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;
  bool* nulls;

  i_eltype = ARR_ELEMTYPE(input);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input, i_eltype, i_typlen, i_typbyval, i_typalign, result,
                    &nulls, n);
}

void getTableName(tableType type, char* name, int bufferSize) {
  char* command;
  int ret;
  int proc;

  const char* function_names[] = {"get_vecs_name_original()",
                                  "get_vecs_name()",
                                  "get_vecs_name_pq_quantization()",
                                  "get_vecs_name_codebook()",
                                  "get_vecs_name_residual_quantization()",
                                  "get_vecs_name_coarse_quantization()",
                                  "get_vecs_name_residual_codebook()",
                                  "get_vecs_name_ivpq_quantization()",
                                  "get_vecs_name_ivpq_codebook()",
                                  "get_vecs_name_coarse_quantization_multi()",
                                  "get_statistics_table()"};

  SPI_connect();

  command = palloc(100 * sizeof(char));
  sprintf(command, "SELECT * FROM %s", function_names[type]);

  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  if (ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    HeapTuple tuple;
    if (proc != 1) {
      elog(ERROR, "Unexpected number of results: %d", proc);
    }
    tuple = tuptable->vals[0];

    snprintf(name, bufferSize, "%s", SPI_getvalue(tuple, tupdesc, 1));
  }
  SPI_finish();
}

void getParameter(parameterType type, int* param) {
  char* command;
  ResultInfo rInfo;
  const char* function_names[] = {"get_pvf()", "get_w()"};

  SPI_connect();

  command = palloc(100 * sizeof(char));
  sprintf(command, "SELECT * FROM %s", function_names[type]);

  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;
  if (rInfo.ret > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable* tuptable = SPI_tuptable;
    HeapTuple tuple;
    if (rInfo.proc != 1) {
      elog(ERROR, "Unexpected number of results: %d", rInfo.proc);
    }
    tuple = tuptable->vals[0];
    *param = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &rInfo.info));
  }
  SPI_finish();
}

// inspired by
// https://stackoverflow.com/questions/9210528/split-string-with-delimiters-in-c?answertab=oldest#tab-top
typedef struct {
  const char* start;
  size_t len;
} token;

char** split(const char* str, char sep) {
  char** array;
  unsigned int start = 0, stop, toks = 0, t;
  token* tokens = palloc((strlen(str) + 1) * sizeof(token));
  for (stop = 0; str[stop]; stop++) {
    if (str[stop] == sep) {
      tokens[toks].start = str + start;
      tokens[toks].len = stop - start;
      toks++;
      start = stop + 1;
    }
  }
  /* Mop up the last token */
  tokens[toks].start = str + start;
  tokens[toks].len = stop - start;
  toks++;
  array = palloc((toks + 1) * sizeof(char*));
  for (t = 0; t < toks; t++) {
    /* Calloc makes it nul-terminated */
    char* token = calloc(tokens[t].len + 1, 1);
    memcpy(token, tokens[t].start, tokens[t].len);
    array[t] = token;
  }
  /* Add a sentinel */
  array[t] = NULL;
  return array;
}

void updateCodebook(float** rawVectors, int rawVectorsSize, int subvectorSize,
                    CodebookWithCounts cb, int cbPositions, int cbCodes,
                    int** nearestCentroids, int* countIncs) {
  float* minDist = palloc(sizeof(float) * cbPositions);
  float** differences = palloc(cbPositions * cbCodes * sizeof(float*));
  float* nearestCentroidRaw = NULL;

  for (int i = 0; i < (cbPositions * cbCodes); i++) {
    differences[i] = palloc(subvectorSize * sizeof(float));
    for (int j = 0; j < subvectorSize; j++) {
      differences[i][j] = 0;
    }
    countIncs[i] = 0;
  }

  for (int i = 0; i < rawVectorsSize; i++) {
    nearestCentroids[i] = palloc(sizeof(int) * cbPositions);
    for (int j = 0; j < cbPositions; j++) {
      minDist[j] = 100;  // sufficient high value
    }
    for (int j = 0; j < cbPositions * cbCodes; j++) {
      int pos = cb[j].pos;
      int code = cb[j].code;
      float* vector = cb[j].vector;
      float dist = squareDistance(rawVectors[i] + (pos * subvectorSize), vector,
                                  subvectorSize);
      if (dist < minDist[pos]) {
        nearestCentroids[i][pos] = code;
        minDist[pos] = dist;
        nearestCentroidRaw = vector;
      }
    }
    for (int j = 0; j < cbPositions; j++) {
      int code = nearestCentroids[i][j];
      countIncs[j * cbCodes + code] += 1;
      for (int k = 0; k < subvectorSize; k++) {
        differences[j * cbCodes + code][k] += nearestCentroidRaw[k];
      }
    }
  }

  // recalculate codebook
  for (int i = 0; i < cbPositions * cbCodes; i++) {
    cb[i].count += countIncs[cb[i].pos * cbCodes + cb[i].code];
    for (int j = 0; j < subvectorSize; j++) {
      cb[i].vector[j] +=
          (1.0 / cb[i].count) * differences[cb[i].pos + cb[i].code][j];
    }
  }
}

void updateCodebookRelation(CodebookWithCounts cb, int cbPositions, int cbCodes,
                            char* tableNameCodebook, int* countIncs,
                            int subvectorSize) {
  char* command;
  char* cur;
  int ret;

  for (int i = 0; i < cbPositions * cbCodes; i++) {
    if (countIncs[cb[i].pos * cbCodes + cb[i].code] > 0) {
      // update codebook entry
      command = palloc(sizeof(char) * (subvectorSize * 16 + 6 + 6 + 100));
      cur = command;
      cur += sprintf(cur, "UPDATE %s SET (vector, count) = (vec_to_bytea('{",
                     tableNameCodebook);
      for (int j = 0; j < subvectorSize; j++) {
        if (j < subvectorSize - 1) {
          cur += sprintf(cur, "%f, ", cb[i].vector[j]);
        } else {
          cur += sprintf(cur, "%f", cb[i].vector[j]);
        }
      }
      cur += sprintf(cur, "}'::float4[]), '%d')", cb[i].count);
      cur += sprintf(cur, " WHERE (pos = %d) AND (code = %d)", cb[i].pos,
                     cb[i].code);
      SPI_connect();
      ret = SPI_exec(command, 0);
      if (ret > 0) {
        SPI_finish();
      }
      pfree(command);
    }
  }
}
void updateProductQuantizationRelation(int** nearestCentroids, char** tokens,
                                       int cbPositions, CodebookWithCounts cb,
                                       char* pqQuantizationTable,
                                       int rawVectorsSize,
                                       int* cqQuantizations) {
  char* command;
  char* cur;
  int ret;
  const char* schema_pq_quantization = "(id, word, vector)";
  const char* schema_fine_quantization = "(id, coarse_id, word, vector)";
  const char* schema_ivpq_quantization = "(id, coarse_id, vector)";

  for (int i = 0; i < rawVectorsSize; i++) {
    command = palloc(sizeof(char) * (100 + cbPositions * 6 + 200));
    cur = command;
    if (cqQuantizations == NULL) {
      cur += sprintf(
          cur, "INSERT INTO %s %s VALUES ((SELECT max(id) + 1 FROM %s), ",
          pqQuantizationTable, schema_pq_quantization, pqQuantizationTable);
      cur += sprintf(cur, "'%s', vec_to_bytea('{", tokens[i]);
    }
    if (tokens == NULL){
      cur += sprintf(
          cur, "INSERT INTO %s %s VALUES ((SELECT max(id) + 1 FROM %s), ",
          pqQuantizationTable, schema_ivpq_quantization, pqQuantizationTable);
      cur += sprintf(cur, "%d, vec_to_bytea('{", cqQuantizations[i]);
    }
    if ((tokens != NULL) && (cqQuantizations != NULL)){
      cur += sprintf(
          cur, "INSERT INTO %s %s VALUES ((SELECT max(id) + 1 FROM %s), ",
          pqQuantizationTable, schema_fine_quantization, pqQuantizationTable);
      cur += sprintf(cur, "%d, '%s', vec_to_bytea('{", cqQuantizations[i],
                     tokens[i]);
    }
    for (int j = 0; j < cbPositions; j++) {
      if (j < (cbPositions - 1)) {
        cur += sprintf(cur, "%d,", nearestCentroids[i][j]);
      } else {
        cur += sprintf(cur, "%d", nearestCentroids[i][j]);
      }
    }
    cur += sprintf(cur, "}'::int2[])");
    cur += sprintf(cur, ")");
    SPI_connect();
    ret = SPI_exec(command, 0);
    if (ret > 0) {
      SPI_finish();
    }
    pfree(command);
  }
}

void updateWordVectorsRelation(char* tableName, char** tokens,
                               float** rawVectors, int rawVectorsSize,
                               int vectorSize) {
  char* command;
  char* cur;
  int ret;
  for (int i = 0; i < rawVectorsSize; i++) {
    command = palloc(sizeof(char) * (100 + vectorSize * 10 + 200));
    cur = command;
    cur += sprintf(cur,
                   "INSERT INTO %s (id, word, vector) VALUES ((SELECT max(id) "
                   "+ 1 FROM %s), '%s', vec_to_bytea('{",
                   tableName, tableName, tokens[i]);
    for (int j = 0; j < vectorSize; j++) {
      if (j < (vectorSize - 1)) {
        cur += sprintf(cur, "%f,", rawVectors[i][j]);
      } else {
        cur += sprintf(cur, "%f", rawVectors[i][j]);
      }
    }
    cur += sprintf(cur, "}'::float4[])");
    cur += sprintf(cur, ")");
    SPI_connect();
    ret = SPI_exec(command, 0);
    if (ret > 0) {
      SPI_finish();
    }

    pfree(command);
  }
}

int compare(const void* a, const void* b) { return (*(int*)a - *(int*)b); }

void convert_bytea_int32(bytea* bstring, int32** output, int* size) {
  int32* ptr = (int32*)VARDATA(bstring);
  if (*size == 0) {  // if size value is given it is assumed that memory is
                     // already allocated
    *output = palloc((VARSIZE(bstring) - VARHDRSZ));
    *size = (VARSIZE(bstring) - VARHDRSZ) / sizeof(int32);
  }
  memcpy(*output, ptr, (*size) * sizeof(int32));
}

void convert_bytea_int16(bytea* bstring, int16** output, int* size) {
  int16* ptr = (int16*)VARDATA(bstring);
  if (*size == 0) {  // if size value is given it is assumed that memory is
                     // already allocated
    *output = palloc((VARSIZE(bstring) - VARHDRSZ));
    *size = (VARSIZE(bstring) - VARHDRSZ) / sizeof(int16);
  }
  memcpy(*output, ptr, (*size) * sizeof(int16));
}

void convert_bytea_float4(bytea* bstring, float4** output, int* size) {
  float4* ptr = (float4*)VARDATA(bstring);
  if (*size == 0) {  // if size value is given it is assumed that memory is
                     // already allocated
    *output = palloc((VARSIZE(bstring) - VARHDRSZ));
    *size = (VARSIZE(bstring) - VARHDRSZ) / sizeof(float4);
  }
  memcpy(*output, ptr, (*size) * sizeof(float4));
}

void convert_float4_bytea(float4* input, bytea** output, int size) {
  *output = (text*)palloc(size * sizeof(float4) + VARHDRSZ);
  SET_VARSIZE(*output, VARHDRSZ + size * sizeof(float4));
  memcpy(VARDATA(*output), input, size * sizeof(float4));
}

void convert_int32_bytea(int32* input, bytea** output, int size) {
  *output = (text*)palloc(size * sizeof(int32) + VARHDRSZ);
  SET_VARSIZE(*output, VARHDRSZ + size * sizeof(int32));
  memcpy(VARDATA(*output), input, size * sizeof(int32));
}

void convert_int16_bytea(int16* input, bytea** output, int size) {
  *output = (text*)palloc(size * sizeof(int16) + VARHDRSZ);
  SET_VARSIZE(*output, VARHDRSZ + size * sizeof(int16));
  memcpy(VARDATA(*output), input, size * sizeof(int16));
}

float computePQDistanceInt16(float* preDists, int16* codes,
                                    int cbPositions, int cbCodes) {
  float distance = 0;
  for (int l = 0; l < cbPositions; l++) {
    distance += preDists[cbCodes * l + codes[l]];
  }
  return distance;
}

void addToTargetList(TargetListElem* targetLists, int queryVectorsIndex,
                            const int target_lists_size, const int method,
                            int16* codes, float4* vector, int wordId) {
  TargetListElem* currentTargetList = targetLists[queryVectorsIndex].last;
  if (method != EXACT_CALC) {
    currentTargetList->codes[currentTargetList->size] = codes;
  }
  currentTargetList->ids[currentTargetList->size] = wordId;
  if ((method == PQ_PV_CALC) || (method == EXACT_CALC)) {
    currentTargetList->vectors[currentTargetList->size] = vector;
  }
  currentTargetList->size += 1;
  if (currentTargetList->size == target_lists_size) {
    currentTargetList->next = palloc(sizeof(TargetListElem));
    if (method != EXACT_CALC) {
      currentTargetList->next->codes =
          palloc(sizeof(int16*) * target_lists_size);
    }
    currentTargetList->next->ids = palloc(sizeof(int) * target_lists_size);
    if ((method == PQ_PV_CALC) || (method == EXACT_CALC)) {
      currentTargetList->next->vectors =
          palloc(sizeof(float4*) * target_lists_size);
    }
    currentTargetList->next->size = 0;
    currentTargetList->next->next = NULL;
    targetLists[queryVectorsIndex].last = currentTargetList->next;
  }
}
