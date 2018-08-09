
// clang-format off

#include "output_utils.h"

// clang-format on

void fillUsrFctx(UsrFctx* usrfctx, TopK topK, int k) {
  usrfctx->tk = topK;
  usrfctx->k = k;
  usrfctx->iter = 0;
  usrfctx->values = (char**)palloc(2 * sizeof(char*));
  usrfctx->values[0] = (char*)palloc(16 * sizeof(char));
  usrfctx->values[1] = (char*)palloc(16 * sizeof(char));
}

void fillUsrFctxBatch(UsrFctxBatch* usrfctx, int* queryIds,
                      int queryVectorsSize, TopK* topKs, int k) {
  usrfctx->tk = topKs;
  usrfctx->k = k;
  usrfctx->queryIds = queryIds;
  usrfctx->queryIdsSize = queryVectorsSize;
  usrfctx->iter = 0;
  usrfctx->values = (char**)palloc(3 * sizeof(char*));
  usrfctx->values[0] = (char*)palloc(16 * sizeof(char));
  usrfctx->values[1] = (char*)palloc(16 * sizeof(char));
  usrfctx->values[2] = (char*)palloc(16 * sizeof(char));
}
