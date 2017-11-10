#include "postgres.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "fmgr.h"
#include "utils/array.h"
#include "math.h"
#include "stdio.h"

#include "catalog/pg_type.h"

#include "cosine_similarity.h"
#include "index_utils.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct UsrFctx {
  TopK tk;
  int k;
  int iter;
  char **values;
} UsrFctx;

PG_FUNCTION_INFO_V1(cosine_similarity);

Datum
cosine_similarity(PG_FUNCTION_ARGS)
{

  ArrayType *input1 = PG_GETARG_ARRAYTYPE_P(0);
  ArrayType *input2 = PG_GETARG_ARRAYTYPE_P(1);

  // scalar type information
  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum *i_data1; // pointer to actual input vector
  Datum *i_data2; // pointer to actual input vector
  bool *nulls;
  int n; // dim of input vector

  double sim; // result (similarity)

  // Decode vector1
  // get type
  i_eltype = ARR_ELEMTYPE(input1);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

  // get array (vector) and dimension n
  deconstruct_array(input1, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data1, &nulls, &n);

  // -------------------------------------------------------------------------------

  // Decode vector2
  // get type
  i_eltype = ARR_ELEMTYPE(input2);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

  // get array (vector) and dimension n
  deconstruct_array(input2, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data2, &nulls, &n);

  sim = cosine_similarity_simple(i_data1, i_data2, n);

  PG_RETURN_FLOAT8(sim);
}

PG_FUNCTION_INFO_V1(cosine_similarity_norm);

Datum
cosine_similarity_norm(PG_FUNCTION_ARGS)
{

  ArrayType *input1 = PG_GETARG_ARRAYTYPE_P(0);
  ArrayType *input2 = PG_GETARG_ARRAYTYPE_P(1);

  // scalar type information
  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum *i_data1; // pointer to actual input vector
  Datum *i_data2; // pointer to actual input vector
  bool *nulls;
  int n; // dim of input vector

  double sim; // result (similarity)

  // Decode vector1
  // get type
  i_eltype = ARR_ELEMTYPE(input1);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

  // get array (vector) and dimension n
  deconstruct_array(input1, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data1, &nulls, &n);

  // -------------------------------------------------------------------------------

  // Decode vector2
  // get type
  i_eltype = ARR_ELEMTYPE(input2);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

  // get array (vector) and dimension n
  deconstruct_array(input2, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data2, &nulls, &n);

  sim = cosine_similarity_simple_norm(i_data1, i_data2, n);

  PG_RETURN_FLOAT8(sim);
}

PG_FUNCTION_INFO_V1(vec_minus);

Datum
vec_minus(PG_FUNCTION_ARGS)
{
  ArrayType *input1;
  ArrayType *input2;

  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum *i_data1; // pointer to actual input vector
  Datum *i_data2; // pointer to actual input vector
  bool *nulls;
  int n = 0; // dim of input vector

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  input1 = PG_GETARG_ARRAYTYPE_P(0);
  input2 = PG_GETARG_ARRAYTYPE_P(1);

  i_eltype = ARR_ELEMTYPE(input1);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input1, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data1, &nulls, &n);

  i_eltype = ARR_ELEMTYPE(input2);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input2, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data2, &nulls, &n);

  dvalues = (Datum*) malloc(sizeof(Datum)*n);

  for (int i = 0; i<n; i++){
    dvalues[i] = Float4GetDatum(DatumGetFloat4(i_data1[i]) - DatumGetFloat4(i_data2[i]));
  }

  dims[0] = n;
  lbs[0] = 1;

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, i_eltype, i_typlen, i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);

}

PG_FUNCTION_INFO_V1(vec_plus);

Datum
vec_plus(PG_FUNCTION_ARGS)
{
  ArrayType *input1;
  ArrayType *input2;

  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum *i_data1; // pointer to actual input vector
  Datum *i_data2; // pointer to actual input vector
  bool *nulls;
  int n = 0; // dim of input vector

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  input1 = PG_GETARG_ARRAYTYPE_P(0);
  input2 = PG_GETARG_ARRAYTYPE_P(1);

  i_eltype = ARR_ELEMTYPE(input1);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input1, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data1, &nulls, &n);

  i_eltype = ARR_ELEMTYPE(input2);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input2, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data2, &nulls, &n);

  dvalues = (Datum*) malloc(sizeof(Datum)*n);

  for (int i = 0; i<n; i++){
    dvalues[i] = Float4GetDatum(DatumGetFloat4(i_data1[i]) + DatumGetFloat4(i_data2[i]));

  }

  dims[0] = n;
  lbs[0] = 1;

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, i_eltype, i_typlen, i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);
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
     Codebook cb;
     int cbPositions = 0;
     int cbCodes = 0;
     float* queryVector;
     int k;

     float* querySimilarities;

     ArrayType* queryArg;
     Datum* queryData;

     Oid eltype;
     int16 typlen;
     bool typbyval;
     char typalign;
     bool *nulls;
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

     queryArg = PG_GETARG_ARRAYTYPE_P(0);
     k = PG_GETARG_INT32(1);

     // get codebook
     cb = getCodebook(&cbPositions, &cbCodes, "pq_codebook");

    // read query from function args
    eltype = ARR_ELEMTYPE(queryArg);
    get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
    deconstruct_array(queryArg, eltype, typlen, typbyval, typalign, &queryData, &nulls, &n);
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
        ArrayType* vectorAt;
        int wordId;
        float distance;

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);
        vectorAt = DatumGetArrayTypeP(vector);
        eltype = ARR_ELEMTYPE(vectorAt);
        get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
        deconstruct_array(vectorAt, eltype, typlen, typbyval, typalign, &data, &nulls, &n);
        distance = 0;
        for (int j = 0; j < n; j++){
          int code = DatumGetInt32(data[j]);
          distance += querySimilarities[j*cbCodes + code];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist);
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

    ArrayType* queryArg;
    Datum* queryData;

    Codebook residualCb;
    int cbPositions = 0;
    int cbCodes = 0;

    CoarseQuantizer cq;
    int cqSize;

    float* queryVector;
    int k;

    float* residualVector;

    Oid eltype;
    int16 typlen;
    bool typbyval;
    char typalign;
    bool *nulls;
    int n = 0;

    float* querySimilarities;

    int ret;
    int proc;
    bool info;
    char command[100];

    TopK topK;
    float maxDist;

    // for coarse quantizer
    float minDist = 1000; // sufficient high value
    int cqId = -1;

    funcctx = SRF_FIRSTCALL_INIT ();
    oldcontext = MemoryContextSwitchTo (funcctx->multi_call_memory_ctx);

    queryArg = PG_GETARG_ARRAYTYPE_P(0);
    k = PG_GETARG_INT32(1);


    // get codebook
    residualCb = getCodebook(&cbPositions, &cbCodes, "residual_codebook");

    // get coarse quantizer
    cq = getCoarseQuantizer(&cqSize);

   // read query from function args
   eltype = ARR_ELEMTYPE(queryArg);
   get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
   deconstruct_array(queryArg, eltype, typlen, typbyval, typalign, &queryData, &nulls, &n);
   queryVector = palloc(n*sizeof(float));
   for (int j=0; j< n; j++){
     queryVector[j] = DatumGetFloat4(queryData[j]);
   }

   // get coarse_quantization(queryVector) (id)
   for (int i=0; i < cqSize; i++){
     float dist = squareDistance(queryVector, cq[i].vector, n);
     if (dist < minDist){
       minDist = dist;
       cqId = i;
     }
   }

   // compute residual = queryVector - coarse_quantization(queryVector)
   residualVector = palloc(n*sizeof(float));
   for (int i = 0; i < n; i++){
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
    topK = palloc(k*sizeof(TopKEntry));
    maxDist = 100.0; // sufficient high value
    for (int i = 0; i < k; i++){
      topK[i].distance = 100.0;
      topK[i].id = -1;
    }

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
        ArrayType* vectorAt;
        int wordId;
        float distance;

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);
        vectorAt = DatumGetArrayTypeP(vector);
        eltype = ARR_ELEMTYPE(vectorAt);
        get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
        deconstruct_array(vectorAt, eltype, typlen, typbyval, typalign, &data, &nulls, &n);
        distance = 0;
        for (int j = 0; j < n; j++){
          int code = DatumGetInt32(data[j]);
          distance += querySimilarities[j*cbCodes + code];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist);
          maxDist = topK[k-1].distance;
        }
      }
      SPI_finish();
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

PG_FUNCTION_INFO_V1(top_k_in_pq);

Datum
top_k_in_pq(PG_FUNCTION_ARGS)
{

  FuncCallContext *funcctx;
  TupleDesc        outtertupdesc;
  TupleTableSlot  *slot;
  AttInMetadata   *attinmeta;
  UsrFctx *usrfctx;

  if (SRF_IS_FIRSTCALL ())
   {


    int k;

    ArrayType* queryArg;
    Datum* queryData;
    float* queryVector;

    ArrayType* idArray;
    Datum* idsData;
    int* inputIds;
    int inputIdSize;

    Oid eltype;
    int16 typlen;
    bool typbyval;
    char typalign;
    bool *nulls;
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

    queryArg = PG_GETARG_ARRAYTYPE_P(0);
    k = PG_GETARG_INT32(1);
    idArray = PG_GETARG_ARRAYTYPE_P(2);

    // read query from function args
    eltype = ARR_ELEMTYPE(queryArg);
    get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
    deconstruct_array(queryArg, eltype, typlen, typbyval, typalign, &queryData, &nulls, &n);
    queryVector = palloc(n*sizeof(float));
    for (int j=0; j< n; j++){
      queryVector[j] = DatumGetFloat4(queryData[j]);
    }

    // read ids from function args
    eltype = ARR_ELEMTYPE(idArray);
    get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
    deconstruct_array(idArray, eltype, typlen, typbyval, typalign, &idsData, &nulls, &n);
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
        ArrayType* vectorAt;
        int wordId;
        float distance;

        HeapTuple tuple = tuptable->vals[i];
        id = SPI_getbinval(tuple, tupdesc, 1, &info);
        vector = SPI_getbinval(tuple, tupdesc, 2, &info);
        wordId = DatumGetInt32(id);
        vectorAt = DatumGetArrayTypeP(vector);
        eltype = ARR_ELEMTYPE(vectorAt);
        get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
        deconstruct_array(vectorAt, eltype, typlen, typbyval, typalign, &data, &nulls, &n);
        distance = 0;
        for (int j = 0; j < n; j++){
          int code = DatumGetInt32(data[j]);
          distance += querySimilarities[j*cbCodes + code];
        }
        if (distance < maxDist){
          updateTopK(topK, distance, wordId, k, maxDist);
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
