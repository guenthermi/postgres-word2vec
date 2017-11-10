#ifndef INDEX_UTILS_H
#define INDEX_UTILS_H

#include "postgres.h"

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

typedef CodebookEntry* Codebook;

typedef CoarseQuantizerEntry* CoarseQuantizer;

void updateTopK(TopK tk, float distance, int id, int k, int maxDist);

float squareDistance(float* v1, float* v2, int n);

CoarseQuantizer getCoarseQuantizer(int* size);

Codebook getCodebook(int* positions, int* codesize, char* tableName);

void freeCodebook(Codebook cb, int size);


#endif /*INDEX_UTILS_H*/
