#ifndef INDEX_UTILS_H
#define INDEX_UTILS_H

#include "postgres.h"
#include "utils/array.h"

typedef struct TopKEntry{
  int id;
  float distance;
}TopKEntry;

typedef TopKEntry* TopK;

typedef struct CoarseQuantizerEntry{
    int id;
    float* vector;
}CoarseQuantizerEntry;

typedef struct CodebookEntry{
  int pos;
  int code;
  float* vector;
} CodebookEntry;

typedef struct WordVectors{
  int* ids;
  float** vectors;
} WordVectors;

typedef struct Blacklist{
  int id;
  bool isValid; // id and next are only valid if isValid = true
  struct Blacklist* next;
} Blacklist;

typedef CodebookEntry* Codebook;

typedef CoarseQuantizerEntry* CoarseQuantizer;

void updateTopK(TopK tk, float distance, int id, int k, int maxDist, int bestPos);

bool inBlacklist(int id, Blacklist* bl);

void addToBlacklist(int id, Blacklist* bl, Blacklist* emptyBl);

float squareDistance(float* v1, float* v2, int n);

void shuffle(int* input, int* output, int inputSize, int outputSize);

CoarseQuantizer getCoarseQuantizer(int* size);

Codebook getCodebook(int* positions, int* codesize, char* tableName);

void freeCodebook(Codebook cb, int size);

WordVectors getVectors(char* tableName, int* ids, int idsSize);

void freeWordVectors(WordVectors vectors, int size);

void getArray(ArrayType* input, Datum** result, int* n);

#endif /*INDEX_UTILS_H*/
