#ifndef INDEX_UTILS_H
#define INDEX_UTILS_H

// clang-format off

#include "postgres.h"
#include "utils/array.h"

// clang-format on

typedef struct ResultInfo {
  int ret;
  int proc;
  bool info;
} ResultInfo;

typedef struct TopKEntry {
  int id;
  float distance;
} TopKEntry;

typedef struct TopKWordEntry {
  char* word;
  float distance;
} TopKWordEntry;

typedef struct TopKPVEntry {
  int id;
  float distance;
  float4* vector;
} TopKPVEntry;

typedef struct QueueEntry {
  int id;
  float distance;
  int positions[2];
} QueueEntry;

typedef struct DistancePQueue {
  QueueEntry* nodes;
  int len;
} DistancePQueue;

typedef TopKEntry* TopK;
typedef TopKPVEntry* TopKPV;

typedef struct CoarseQuantizerEntry {
  int id;
  float* vector;
} CoarseQuantizerEntry;

typedef struct CodebookEntry {
  int pos;
  int code;
  float* vector;
} CodebookEntry;

typedef struct CodebookEntryComplete {
  int pos;
  int code;
  float* vector;
  int count;
} CodebookEntryComplete;

typedef struct WordVectors {
  int* ids;
  float** vectors;
} WordVectors;

typedef struct Blacklist {
  int id;
  bool isValid;  // id and next are only valid if isValid = true
  struct Blacklist* next;
} Blacklist;

typedef struct TargetListElem {
  int16** codes;
  int* ids;
  float4** vectors;
  int size;
  struct TargetListElem* next;
  struct TargetListElem* last;
} TargetListElem;

typedef TargetListElem* TargetLists;

typedef enum {
  ORIGINAL,
  NORMALIZED,
  PQ_QUANTIZATION,
  CODEBOOK,
  RESIDUAL_QUANTIZATION,
  COARSE_QUANTIZATION,
  RESIDUAL_CODEBOOK,
  IVPQ_QUANTIZATION,
  IVPQ_CODEBOOK,
  COARSE_QUANTIZATION_MULTI,
  STATISTICS
} tableType;

typedef enum { PARAM_PVF, PARAM_W } parameterType;

typedef enum { PQ_CALC = 0, EXACT_CALC = 1, PQ_PV_CALC = 2 } calculationMethod;

typedef CodebookEntry* Codebook;

typedef CodebookEntryComplete* CodebookWithCounts;

typedef CoarseQuantizerEntry* CoarseQuantizer;

typedef struct CodebookCompound {
  Codebook codebook;
  int codeSize;
  int positions;
} CodebookCompound;

void updateTopK(TopK tk, float distance, int id, int k, int maxDist);

void updateTopKPV(TopKPV tk, float distance, int id, int k, int maxDist,
                  float4* vector, int dim);

void updateTopKWordEntry(char** term, char* word);

void initTopK(TopK* pTopK, int k, const float maxDist);

void initTopKs(TopK** pTopKs, float** pMaxDists, int queryVectorsSize, int k,
               const float maxDist);

void initTopKPV(TopKPV* pTopK, int k, const float maxDist, int dim);

void initTopKPVs(TopKPV** pTopKs, float** pMaxDists, int queryVectorsSize,
                 int k, const float maxDist, int dim);

QueueEntry pop(DistancePQueue* queue);

void push(DistancePQueue* queue, float distance, int id, int positions[2]);

int cmpTopKPVEntry(const void* a, const void* b);

int cmpTopKEntry(const void* a, const void* b);

bool inBlacklist(int id, Blacklist* bl);

void addToBlacklist(int id, Blacklist* bl, Blacklist* emptyBl);

bool determineCoarseIdsMultiWithStatistics(
    int*** pCqIds, int*** pCqTableIds, int** pCqTableIdCounts,
    int* queryVectorsIndices, int queryVectorsIndicesSize, int queryVectorsSize,
    float maxDist, CoarseQuantizer cq, int cqSize, float4** queryVectors,
    int queryDim, float* statistics, int inputIdsSize, const int minTargetCount,
    const float confidence);

bool determineCoarseIdsMultiWithStatisticsMulti(
    int*** pCqIds, int*** pCqTableIds, int** pCqTableIdCounts,
    int* queryVectorsIndices, int queryVectorsIndicesSize, int queryVectorsSize,
    float maxDist, Codebook cq, int cqSize, int cqPositions, int cqCodes,
    float4** queryVectors, int queryDim, float* statistics, int inputIdsSize,
    const int minTargetCount, float confidence);

void postverify(int* queryVectorsIndices, int queryVectorsIndicesSize, int k,
                int pvf, TopKPV* topKPVs, TopK* topKs, float4** queryVectors,
                int queryDim, const float maxDistance);

void getPrecomputedDistances(float4* preDists, int cbPositions, int cbCodes,
                             int subvectorSize, float4* queryVector,
                             Codebook cb);

void getPrecomputedDistancesDouble(float4* preDists, int cbPositions,
                                   int cbCodes, int subvectorSize,
                                   float4* queryVector, Codebook cb);

float squareDistance(float* v1, float* v2, int n);

void shuffle(int* input, int* output, int inputSize, int outputSize);

float getConfidenceBin(int expect, int size, float p);

float getConfidenceHyp(int expect, int size, float p, int stat_size);

float* getStatistics(void);

CoarseQuantizer getCoarseQuantizer(int* size);

CodebookCompound getCodebook(char* tableName);

CodebookWithCounts getCodebookWithCounts(int* positions, int* codesize,
                                         char* tableName);

WordVectors getVectors(char* tableName, int* ids, int idsSize);

void getArray(ArrayType* input, Datum** result, int* n);

void getTableName(tableType type, char* name, int bufferSize);

void getParameter(parameterType type, int* param);

char** split(const char* str, char sep);

void updateCodebook(float** rawVectors, int rawVectorsSize, int subvectorSize,
                    CodebookWithCounts cb, int cbPositions, int cbCodes,
                    int** nearestCentroids, int* countIncs);

void updateCodebookRelation(CodebookWithCounts cb, int cbPositions, int cbCodes,
                            char* tableNameCodebook, int* countIncs,
                            int subvectorSize);

void updateProductQuantizationRelation(int** nearestCentroids, char** tokens,
                                       int cbPositions, CodebookWithCounts cb,
                                       char* pqQuantizationTable,
                                       int rawVectorsSize,
                                       int* cqQuantizations);

void updateWordVectorsRelation(char* tableName, char** tokens,
                               float** rawVectors, int rawVectorsSize,
                               int vectorSize);

int compare(const void* a, const void* b);

void convert_bytea_int32(bytea* bstring, int32** output, int32* size);

void convert_bytea_int16(bytea* bstring, int16** output, int* size);

void convert_bytea_float4(bytea* bstring, float4** output, int* size);

void convert_int32_bytea(int32* input, bytea** output, int size);

void convert_int16_bytea(int16* input, bytea** output, int size);

void convert_float4_bytea(float4* input, bytea** output, int size);

float computePQDistanceInt16(float* preDists, int16* codes,
                                    int cbPositions, int cbCodes);

void addToTargetList(TargetListElem* targetLists, int queryVectorsIndex,
                            const int target_lists_size, const int method,
                            int16* codes, float4* vector, int wordId);

#endif /*INDEX_UTILS_H*/
