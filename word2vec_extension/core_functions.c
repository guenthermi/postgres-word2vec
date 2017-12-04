#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"

#include "cosine_similarity.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

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

  dvalues = (Datum*) palloc(sizeof(Datum)*n);

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

  dvalues = (Datum*) palloc(sizeof(Datum)*n);

  for (int i = 0; i<n; i++){
    dvalues[i] = Float4GetDatum(DatumGetFloat4(i_data1[i]) + DatumGetFloat4(i_data2[i]));

  }

  dims[0] = n;
  lbs[0] = 1;

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, i_eltype, i_typlen, i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);
}

PG_FUNCTION_INFO_V1(vec_normalize);

Datum
vec_normalize(PG_FUNCTION_ARGS)
{
  ArrayType *input;

  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum *i_data; // pointer to actual input vector
  bool *nulls;
  int n = 0; // dim of input vector

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  float sq_length = 0;
  float length = 0;

  input = PG_GETARG_ARRAYTYPE_P(0);

  i_eltype = ARR_ELEMTYPE(input);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input, i_eltype, i_typlen, i_typbyval, i_typalign, &i_data, &nulls, &n);

  dvalues = (Datum*) palloc(sizeof(Datum)*n);

  // determine square length
  for (int i = 0; i<n; i++){
    sq_length += DatumGetFloat4(i_data[i])* DatumGetFloat4(i_data[i]);
  }
  length = sqrt(sq_length);

  // normalize
  for (int i = 0; i<n; i++){
    dvalues[i] = Float4GetDatum(DatumGetFloat4(i_data[i]) / length);

  }

  dims[0] = n;
  lbs[0] = 1;

  v = construct_md_array(dvalues, NULL, 1, dims, lbs, i_eltype, i_typlen, i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);
}

PG_FUNCTION_INFO_V1(centroid);

Datum
centroid(PG_FUNCTION_ARGS)
{

  typedef struct{
      int16       elmlen;
      bool        elmbyval;
      char        elmalign;
      Oid         elmtype;
  } elm_info;

  elm_info info;
  int numelems;

  AnyArrayType *arr;

  int input_size;

  int nextelem = 0;

  array_iter iter;

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  int vec_size;
  float* output;

  arr = PG_GETARG_ARRAYTYPE_P(0);

  array_iter_setup(&iter, arr);
  numelems = ArrayGetNItems(AARR_NDIM(arr), AARR_DIMS(arr));

  info.elmtype = AARR_ELEMTYPE(arr);

  get_typlenbyvalalign(info.elmtype, &info.elmlen, &info.elmbyval,  &info.elmalign);
  if (AARR_NDIM(arr) != 2){
    elog(ERROR, "Input is not a 2-dimensional array");
  }

  vec_size = AARR_DIMS(arr)[1];
  output = palloc(sizeof(float)*vec_size);
  input_size = numelems / vec_size;

  for (int i = 0; i < vec_size; i++){
    output[i] = 0;
  }

  while (nextelem < numelems)
  {
      int offset = nextelem++;
      Datum elem;

      elem = array_iter_next(&iter, &fcinfo->isnull, offset, info.elmlen, info.elmbyval, info.elmalign);
      output[offset % vec_size] += DatumGetFloat4(elem) / (float) input_size;
  }

  dvalues = (Datum*) malloc(sizeof(Datum)*vec_size);

  for (int i = 0; i < vec_size; i++){
    dvalues[i] = Float4GetDatum(output[i]);
  }

  dims[0] = vec_size;
  lbs[0] = 1;
  v = construct_md_array(dvalues, NULL, 1, dims, lbs, info.elmtype, info.elmlen, info.elmbyval, info.elmalign);



PG_RETURN_ARRAYTYPE_P(v);

}
