#include "postgres.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "stdio.h"
#include "stdlib.h"

#include "catalog/pg_type.h"

#include "index_utils.h"

typedef struct UsrFctx {
  TopK tk;
  int k;
  int iter;
  char **values;
} UsrFctx;

typedef struct UsrFctxCluster {
  int* ids;
  int size;
  int* nearestCentroid;
  float** centroids;
  int iter;
  int k; // number of clusters
  char **values;
} UsrFctxCluster;

typedef struct UsrFctxGrouping {
  int* ids;
  int size;
  int* nearestGroup;
  float** groupVecs;
  int iter;
  int groupsSize; // number of groups
  char **values;
} UsrFctxGrouping;

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
     Codebook cb;
     int cbPositions = 0;
     int cbCodes = 0;
     float* queryVector;
     int k;

     float* querySimilarities;

     Datum* queryData;
     int n = 0;

     MemoryContext  oldcontext;

     char *command;
     int ret;
     int proc;
     bool info;

     TopK topK;
     float maxDist;

     funcctx = SRF_FIRSTCALL_INIT ();
     oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

     k = PG_GETARG_INT32(1);

     // get codebook
     cb = getCodebook(&cbPositions, &cbCodes, "pq_codebook");

    // read query from function args
    getArray(PG_GETARG_ARRAYTYPE_P(0), &queryData, &n);

    queryVector = palloc(n*sizeof(float));
    for (int j=0; j< n; j++){
      queryVector[j] = DatumGetFloat4(queryData[j]);
    }

    // determine similarities of codebook entries to query vector
    querySimilarities = palloc(cbPositions*cbCodes*sizeof(float));
    for (int i=0; i< cbPositions*cbCodes; i++){
        int pos = cb[i].pos;
        int code = cb[i].code;
        float* vector = cb[i].vector;
        querySimilarities[pos*cbCodes + code] = squareDistance(queryVector+(pos*25), vector, 25);
    }
    // calculate TopK by summing up squared distanced sum method
    topK = palloc(k*sizeof(TopKEntry));
    maxDist = 100.0; // sufficient high value
    for (int i = 0; i < k; i++){
      topK[i].distance = 100.0;
      topK[i].id = -1;
    }

    SPI_connect();
    command = "SELECT id, vector FROM pq_quantization";
    ret = SPI_exec(command, 0);
    proc = SPI_processed;
    if (ret > 0 && SPI_tuptable != NULL){
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;
      int i;
      for (i = 0; i < proc; i++){
        Datum id;
        Datum vector;
        Datum* data;
        int wordId;
        float distance;

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);

        getArray(DatumGetArrayTypeP(vector), &data, &n);

        distance = 0;
        for (int j = 0; j < n; j++){
          int code = DatumGetInt32(data[j]);
          distance += querySimilarities[j*cbCodes + code];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist, 0);
          maxDist = topK[k-1].distance;
        }
      }
      SPI_finish();

    }

    freeCodebook(cb,cbPositions * cbCodes);

    usrfctx = (UsrFctx*) palloc (sizeof (UsrFctx));
    usrfctx -> tk = topK;
    usrfctx -> k = k;
    usrfctx -> iter = 0;
    usrfctx -> values = (char **) palloc (2 * sizeof (char *));
    usrfctx -> values  [0] = (char*) palloc   (16 * sizeof (char));
    usrfctx -> values  [1] = (char*) palloc  (16 * sizeof (char));
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

    MemoryContext  oldcontext;

    Datum* queryData;

    Codebook residualCb;
    int cbPositions = 0;
    int cbCodes = 0;

    CoarseQuantizer cq;
    int cqSize;

    float* queryVector;
    int k;
    int queryDim;

    float* residualVector;

    int n = 0;

    float* querySimilarities;

    int ret;
    int proc;
    bool info;
    char command[100];

    TopK topK;
    float maxDist;

    // for coarse quantizer
    float minDist; // sufficient high value
    int cqId = -1;

    int bestPos;
    int foundInstances;
    Blacklist bl;

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    k = PG_GETARG_INT32(1);


    // get codebook
    residualCb = getCodebook(&cbPositions, &cbCodes, "residual_codebook");

    // get coarse quantizer
    cq = getCoarseQuantizer(&cqSize);

   // read query from function args
   getArray(PG_GETARG_ARRAYTYPE_P(0), &queryData, &n);
   queryVector = palloc(n*sizeof(float));
   queryDim = n;
   for (int j=0; j< queryDim; j++){
     queryVector[j] = DatumGetFloat4(queryData[j]);
   }

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

     bestPos = foundInstances;

     // get coarse_quantization(queryVector) (id)
     minDist = 1000;
     for (int i=0; i < cqSize; i++){
       float dist;

       if (inBlacklist(i, &bl)){
         continue;
       }
       dist = squareDistance(queryVector, cq[i].vector, n);
       if (dist < minDist){
         minDist = dist;
         cqId = i;
       }
     }

     // add coarse quantizer to Blacklist
     newBl = palloc(sizeof(Blacklist));
     newBl->isValid = false;

     addToBlacklist(cqId, &bl, newBl);

     // compute residual = queryVector - coarse_quantization(queryVector)
     residualVector = palloc(queryDim*sizeof(float));
     for (int i = 0; i < queryDim; i++){
       residualVector[i] = queryVector[i] - cq[cqId].vector[i];
     }

     // compute subvector similarities lookup
     // determine similarities of codebook entries to residual vector
     querySimilarities = palloc(cbPositions*cbCodes*sizeof(float));
     for (int i=0; i< cbPositions*cbCodes; i++){
         int pos = residualCb[i].pos;
         int code = residualCb[i].code;
         float* vector = residualCb[i].vector;
         querySimilarities[pos*cbCodes + code] = squareDistance(residualVector+(pos*25), vector, 25);
     }

      // calculate TopK by summing up squared distanced sum method

      // connect to databse and compute approximated similarities with sum method
      SPI_connect();
      sprintf(command, "SELECT id, vector FROM fine_quantization WHERE coarse_id = %d", cq[cqId].id);
      ret = SPI_exec(command, 0);
      proc = SPI_processed;
      if (ret > 0 && SPI_tuptable != NULL){
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        int i;
        for (i = 0; i < proc; i++){
          Datum id;
          Datum vector;
          Datum* data;
          int wordId;
          float distance;

          HeapTuple tuple = tuptable->vals[i];
          id = SPI_getbinval(tuple, tupdesc, 1, &info);
          vector = SPI_getbinval(tuple, tupdesc, 2, &info);
          wordId = DatumGetInt32(id);
          getArray(DatumGetArrayTypeP(vector), &data, &n);
          distance = 0;
          for (int j = 0; j < n; j++){
            int code = DatumGetInt32(data[j]);
            distance += querySimilarities[j*cbCodes + code];
          }
          if (distance < maxDist){
            updateTopK(topK, distance, wordId, k, maxDist, bestPos);
            maxDist = topK[k-1].distance;
          }
        }
        SPI_finish();
      }

      foundInstances += proc;

    }

    freeCodebook(residualCb,cbPositions * cbCodes);

    usrfctx = (UsrFctx*) palloc (sizeof (UsrFctx));
    usrfctx -> tk = topK;
    usrfctx -> k = k;
    usrfctx -> iter = 0;
    usrfctx -> values = (char **) palloc (2 * sizeof (char *));
    usrfctx -> values  [0] = (char*) palloc   (16 * sizeof (char));
    usrfctx -> values  [1] = (char*) palloc  (16 * sizeof (char));
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

    Datum* queryData;
    float* queryVector;

    Datum* idsData;
    int* inputIds;
    int inputIdSize;

    int n = 0;

    Codebook cb;
    int cbPositions = 0;
    int cbCodes = 0;

    float* querySimilarities;
    TopK topK;
    float maxDist;

    int ret;
    int proc;
    bool info;
    char* command;
    char * cur;

    MemoryContext  oldcontext;

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    k = PG_GETARG_INT32(1);

    // read query from function args
    getArray(PG_GETARG_ARRAYTYPE_P(0), &queryData, &n);
    queryVector = palloc(n*sizeof(float));
    for (int j=0; j< n; j++){
      queryVector[j] = DatumGetFloat4(queryData[j]);
    }

    // read ids from function args
    getArray(PG_GETARG_ARRAYTYPE_P(2), &idsData, &n);
    inputIds = palloc(n*sizeof(float));
    for (int j=0; j< n; j++){
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdSize = n;

    // get pq codebook
    cb = getCodebook(&cbPositions, &cbCodes, "pq_codebook");

    // determine similarities of codebook entries to query vector
    querySimilarities = palloc(cbPositions*cbCodes*sizeof(float));
    for (int i=0; i< cbPositions*cbCodes; i++){
      int pos = cb[i].pos;
      int code = cb[i].code;
      float* vector = cb[i].vector;
      querySimilarities[pos*cbCodes + code] = squareDistance(queryVector+(pos*25), vector, 25);
    }

    // calculate TopK by summing up squared distanced sum method
    topK = palloc(k*sizeof(TopKEntry));
    maxDist = 100.0; // sufficient high value
    for (int i = 0; i < k; i++){
      topK[i].distance = 100.0;
      topK[i].id = -1;
    }

    // get codes for all entries with an id in inputIds -> SQL Query
    SPI_connect();
    command = palloc(60* sizeof(char) + inputIdSize*8*sizeof(char));
    sprintf(command, "SELECT id, vector FROM pq_quantization WHERE id IN (");
    // fill command
    cur = command + strlen(command); // TODO check if this works
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
        Datum* data;
        int wordId;
        float distance;

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);
        getArray(DatumGetArrayTypeP(vector), &data, &n);
        distance = 0;
        for (int j = 0; j < n; j++){
          int code = DatumGetInt32(data[j]);
          distance += querySimilarities[j*cbCodes + code];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist, 0);
          maxDist = topK[k-1].distance;
        }
      }
      SPI_finish();

      usrfctx = (UsrFctx*) palloc (sizeof (UsrFctx));
      usrfctx -> tk = topK;
      usrfctx -> k = k;
      usrfctx -> iter = 0;
      usrfctx -> values = (char **) palloc (2 * sizeof (char *));
      usrfctx -> values  [0] = (char*) palloc   (16 * sizeof (char));
      usrfctx -> values  [1] = (char*) palloc  (16 * sizeof (char));
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

    freeCodebook(cb,cbPositions * cbCodes);
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
    cb = getCodebook(&cbPositions, &cbCodes, "pq_codebook");

    // choose initial km-centroid randomly
    kmCentroidIds = palloc(sizeof(int)*k);
    shuffle(inputIds, kmCentroidIds, inputIdsSize, k);

    // get vectors for ids
    idVectors = getVectors("google_vecs_norm", kmCentroidIds, k);
    kmCentroids = idVectors.vectors;

    kmCentroidsNew = palloc(sizeof(float*)*k);
    for (int i = 0; i < k; i++){
      kmCentroidsNew[i] = palloc(sizeof(float)*300);
    }


    for (int iteration = 0; iteration < iterations; iteration++){

      int ret;
      int proc;
      bool info;
      char* command;
      char * cur;

      // init kmCentroidsNew
      for (int i=0; i<k;i++){
        for (int j = 0; j < 300; j++){
          kmCentroidsNew[i][j] = 0;
        }
      }

      // determine similarities of codebook entries to km_centroid vector
      querySimilarities = palloc(sizeof(float*) * k);

      for (int cs = 0; cs < k; cs++){
        querySimilarities[cs] = palloc(cbPositions*cbCodes*sizeof(float));
        for (int i=0; i< cbPositions*cbCodes; i++){
          int pos = cb[i].pos;
          int code = cb[i].code;
          float* vector = cb[i].vector;
          querySimilarities[cs][pos*cbCodes + code] = squareDistance(kmCentroids[cs]+(pos*25), vector, 25);
        }
      }


      // reset counts for relation
      for (int i = 0; i < k; i++){
        relationCounts[i] = 0;
      }

      // get vectors for ids
      // get codes for all entries with an id in inputIds -> SQL Query
      SPI_connect();
      command = palloc(200* sizeof(char) + inputIdsSize*8*sizeof(char));
      sprintf(command, "SELECT pq_quantization.id, pq_quantization.vector, google_vecs_norm.vector FROM pq_quantization INNER JOIN google_vecs_norm ON google_vecs_norm.id = pq_quantization.id WHERE pq_quantization.id IN (");
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

          Datum* dataPqVector;
          Datum* dataBigVector;

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

          getArray(DatumGetArrayTypeP(pqVector), &dataPqVector, &n);
          pqSize = n;

          getArray(DatumGetArrayTypeP(bigVector), &dataBigVector, &n);



          for (int centroidIndex = 0; centroidIndex < k; centroidIndex++){
            distance = 0;
            for (int j = 0; j < pqSize; j++){
              int code = DatumGetInt32(dataPqVector[j]);
              distance += querySimilarities[centroidIndex][j*cbCodes + code];
            }

            if (distance < minDist){
              minDist = distance;
              nearestCentroid[wordId] = centroidIndex;
            }
          }
          relationCounts[nearestCentroid[wordId]]++;
          for (int j = 0; j < 300; j++){
            kmCentroidsNew[nearestCentroid[wordId]][j] += DatumGetFloat4(dataBigVector[j]);
          }
        }
        SPI_finish();
      }
      // calculate new km-centroids
      for (int cs = 0; cs < k; cs++){
        for (int pos = 0; pos < 300; pos++){
          if (relationCounts[cs] > 0){
            kmCentroids[cs][pos] = kmCentroidsNew[cs][pos] / relationCounts[cs];
          }else{
            kmCentroids[cs][pos] = 0;
          }
          kmCentroidsNew[cs][pos] = kmCentroids[cs][pos];
        }
      }
    }

    freeWordVectors(idVectors, k);

    usrfctx = (UsrFctxCluster*) palloc (sizeof (UsrFctxCluster));
    usrfctx -> ids = inputIds;
    usrfctx -> size = inputIdsSize;
    usrfctx -> nearestCentroid = nearestCentroid;
    usrfctx -> centroids = kmCentroidsNew;
    usrfctx -> iter = 0;
    usrfctx -> k = k;

    usrfctx -> values = (char **) palloc (2 * sizeof (char *));
    usrfctx -> values  [0] = (char*) palloc   ((18 * 300 + 4) * sizeof (char));
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
    for (int i = 0; i < 300; i++){
      if (i < 299){
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

PG_FUNCTION_INFO_V1(grouping_pq_to_id);

Datum
grouping_pq_to_id(PG_FUNCTION_ARGS)
{
  // input: array of ids to cluster, array of vectors for groups
  // output: rows (id, group_vector)

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctxGrouping *usrfctx;

  if (SRF_IS_FIRSTCALL ()){

    Oid eltype;
    int16 typlen;
    bool typbyval;
    char typalign;
    //bool *nulls;
    int n = 0;

    MemoryContext  oldcontext;

    Datum* idsData;
    AnyArrayType* groupidArray;

    int numelems;
    int nextelem = 0;
    array_iter iter;

    int* inputIds;
    int inputIdsSize;

    float** groupVecs;
    int groupVecsSize;

    float** querySimilarities;

    Codebook cb;
    int cbPositions = 0;
    int cbCodes = 0;

    int* nearestGroup;

    int ret;
    int proc;
    bool info;
    char* command;
    char * cur;

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    groupidArray = PG_GETARG_ARRAYTYPE_P(1);

    // read ids from function args

    getArray(PG_GETARG_ARRAYTYPE_P(0), &idsData, &n);
    inputIds = palloc(n*sizeof(int));
    for (int j=0; j< n; j++){
      inputIds[j] = DatumGetInt32(idsData[j]);
    }
    inputIdsSize = n;

    array_iter_setup(&iter, groupidArray);
    numelems = ArrayGetNItems(AARR_NDIM(groupidArray), AARR_DIMS(groupidArray));

    eltype = AARR_ELEMTYPE(groupidArray);
    get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
    if (AARR_NDIM(groupidArray) != 2){
      elog(ERROR, "Input is not a 2-dimensional array");
    }

    n = AARR_DIMS(groupidArray)[1];
    groupVecs = palloc(sizeof(float*)*n);
    groupVecsSize = numelems / n;

    while (nextelem < numelems)
    {
        int offset = nextelem++;
        Datum elem;
        elem = array_iter_next(&iter, &fcinfo->isnull, offset, typlen, typbyval, typalign);
        if ((offset % n) == 0){
          groupVecs[offset / n] = palloc(sizeof(float)*n);
        }
        groupVecs[offset / n][offset % n] = DatumGetFloat4(elem);
    }

    nearestGroup = palloc(sizeof(int)*(inputIdsSize));

    // get pq codebook
    cb = getCodebook(&cbPositions, &cbCodes, "pq_codebook");

    querySimilarities = palloc(sizeof(float*) * groupVecsSize);

    for (int cs = 0; cs < groupVecsSize; cs++){
      querySimilarities[cs] = palloc(cbPositions*cbCodes*sizeof(float));
      for (int i=0; i< cbPositions*cbCodes; i++){
        int pos = cb[i].pos;
        int code = cb[i].code;
        float* vector = cb[i].vector;
        querySimilarities[cs][pos*cbCodes + code] = squareDistance(groupVecs[cs]+(pos*25), vector, 25);
      }
    }
    // get vectors for group_ids
    // get codes for all entries with an id in inputIds -> SQL Query
    SPI_connect();
    command = palloc(200* sizeof(char) + inputIdsSize*8*sizeof(char));
    sprintf(command, "SELECT pq_quantization.id, pq_quantization.vector FROM pq_quantization WHERE pq_quantization.id IN (");
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

        Datum* dataPqVector;

        float distance;
        int pqSize;

        // variables to determine best match
        float minDist = 100; // sufficient high value

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        pqVector = SPI_getbinval(tuple, tupdesc, 2, &info);

        inputIds[i] = DatumGetInt32(id);
        getArray(DatumGetArrayTypeP(pqVector), &dataPqVector, &n);
        pqSize = n;


        for (int groupIndex = 0; groupIndex < groupVecsSize; groupIndex++){
          distance = 0;
          for (int j = 0; j < pqSize; j++){
            int code = DatumGetInt32(dataPqVector[j]);
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
    usrfctx -> groupVecs = groupVecs;
    usrfctx -> iter = 0;
    usrfctx -> groupsSize = groupVecsSize;
    usrfctx -> values = (char **) palloc (2 * sizeof (char *));
    usrfctx -> values  [0] = (char*) palloc  ((18) * sizeof (char));
    usrfctx -> values  [1] = (char*) palloc   ((18 * 300 + 4) * sizeof (char));
    funcctx -> user_fctx = (void *)usrfctx;
    outtertupdesc = CreateTemplateTupleDesc (2 , false);
    TupleDescInitEntry (outtertupdesc,  1, "Ids",    INT4OID, -1, 0);
    TupleDescInitEntry (outtertupdesc,  2, "Vectors",FLOAT4ARRAYOID,  -1, 0);
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
    char* cursor;

    sprintf(usrfctx->values[0], "%d", usrfctx->ids[usrfctx->iter]);

    sprintf(usrfctx->values[1], "{ ");
    cursor = usrfctx->values[1] + strlen("{ ");
    for (int i = 0; i < 300; i++){
      cursor += sprintf(cursor, " %f,", usrfctx -> groupVecs[usrfctx->nearestGroup[usrfctx->iter]][i]);
    }
    sprintf(cursor-1, "}");

    usrfctx->iter++;
    outTuple = BuildTupleFromCStrings (funcctx -> attinmeta,
              usrfctx -> values);
    result = TupleGetDatum (funcctx -> slot, outTuple);
    SRF_RETURN_NEXT(funcctx, result);

  }
}
