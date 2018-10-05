#ifndef OUTPUT_UTILS_H
#define OUTPUT_UTILS_H

// clang-format off

#include "index_utils.h"

// clang-format on

typedef struct UsrFctx {
  TopK tk;
  int k;
  int iter;
  char** values;
} UsrFctx;

typedef struct UsrFctxBatch {
  TopK* tk;
  int k;
  int iter;
  char** values;
  int* queryIds;
  int queryIdsSize;
} UsrFctxBatch;

typedef struct UsrFctxCluster {
  int* ids;
  int size;
  int* nearestCentroid;
  float** centroids;
  int iter;
  int k;  // number of clusters
  char** values;
} UsrFctxCluster;

typedef struct UsrFctxGrouping {
  int* ids;
  int size;
  int* nearestGroup;
  int* groups;
  int iter;
  int groupsSize;  // number of groups
  char** values;
} UsrFctxGrouping;

void fillUsrFctx(UsrFctx* usrfctx, TopK topK, int k);
void fillUsrFctxBatch(UsrFctxBatch* usrfctx, int* queryIds,
                      int queryVectorsSize, TopK* topKs, int k);

#endif /*OUTPUT_UTILS_H*/
