#ifndef INDEX_UTILS_H
#define INDEX_UTILS_H

#include "postgres.h"
#include "utils/array.h"

typedef struct TopKEntry{
  int id;
  float distance;
}TopKEntry;

typedef struct TopKWordEntry{
  char* word;
  float distance;
}TopKWordEntry;

typedef TopKEntry* TopK;
typedef TopKWordEntry* TopKCplx;

typedef struct CoarseQuantizerEntry{
    int id;
    float* vector;
}CoarseQuantizerEntry;


typedef struct CodebookEntry{
  int pos;
  int code;
  float* vector;
} CodebookEntry;

typedef struct CodebookEntryComplete{
  int pos;
  int code;
  float* vector;
  int count;
} CodebookEntryComplete;

typedef struct WordVectors{
  int* ids;
  float** vectors;
} WordVectors;

typedef struct Blacklist{
  int id;
  bool isValid; // id and next are only valid if isValid = true
  struct Blacklist* next;
} Blacklist;

typedef enum {ORIGINAL, NORMALIZED, PQ_QUANTIZATION, CODEBOOK, RESIDUAL_QUANTIZATION, COARSE_QUANTIZATION, RESIDUAL_CODBOOK} tableType;

typedef CodebookEntry* Codebook;

typedef CodebookEntryComplete* CodebookWithCounts;

typedef CoarseQuantizerEntry* CoarseQuantizer;

void updateTopK(TopK tk, float distance, int id, int k, int maxDist);

void updateTopKCplx(TopKCplx tk, float distance, char**, int k, int maxDist);

void updateTopKWordEntry(char** term, char* word);

bool inBlacklist(int id, Blacklist* bl);

void addToBlacklist(int id, Blacklist* bl, Blacklist* emptyBl);

float squareDistance(float* v1, float* v2, int n);

void shuffle(int* input, int* output, int inputSize, int outputSize);

CoarseQuantizer getCoarseQuantizer(int* size);

Codebook getCodebook(int* positions, int* codesize, char* tableName);

CodebookWithCounts getCodebookWithCounts(int* positions, int* codesize, char* tableName);

void freeCodebook(Codebook cb, int size);

void freeCodebookWithCounts(CodebookWithCounts cb, int size);

WordVectors getVectors(char* tableName, int* ids, int idsSize);

void freeWordVectors(WordVectors vectors, int size);

void getArray(ArrayType* input, Datum** result, int* n);

void getTableName(tableType type, char* name, int bufferSize);

char **split(const char *str, char sep);

void updateCodebook(float** rawVectors, int rawVectorsSize, int subvectorSize, CodebookWithCounts cb, int cbPositions, int cbCodes, int** nearestCentroids, int* countIncs);

void updateCodebookRelation(CodebookWithCounts cb, int cbPositions, int cbCodes, char* tableNameCodebook, int* countIncs, int subvectorSize);

void updateProductQuantizationRelation(int** nearestCentroids, char** tokens, int cbPositions, CodebookWithCounts cb, char* pqQuantizationTable, int rawVectorsSize, int* cqQuantizations);

void updateWordVectorsRelation(char* tableName, char** tokens, float** rawVectors, int rawVectorsSize, int vectorSize);

#endif /*INDEX_UTILS_H*/
