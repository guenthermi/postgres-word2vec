// clang-format off

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "catalog/pg_type.h"

#include "cosine_similarity.h"

#include "index_utils.h"

// clang-format on

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(cosine_similarity);

Datum cosine_similarity(PG_FUNCTION_ARGS) {
  Datum* i_data1;  // pointer to actual input vector
  Datum* i_data2;  // pointer to actual input vector

  int n;  // dim of input vector

  double sim;  // result (similarity)

  // get array (vector) and dimension n
  getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data1, &n);

  // get array (vector) and dimension n
  getArray(PG_GETARG_ARRAYTYPE_P(1), &i_data2, &n);

  sim = cosine_similarity_simple(i_data1, i_data2, n);

  PG_RETURN_FLOAT8(sim);
}

PG_FUNCTION_INFO_V1(cosine_similarity_norm);

Datum cosine_similarity_norm(PG_FUNCTION_ARGS) {
  Datum* i_data1;  // pointer to actual input vector
  Datum* i_data2;  // pointer to actual input vector

  int n;  // dim of input vector

  double sim;  // result (similarity)

  // get array (vector) and dimension n
  getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data1, &n);

  // get array (vector) and dimension n
  getArray(PG_GETARG_ARRAYTYPE_P(1), &i_data2, &n);

  sim = cosine_similarity_simple_norm(i_data1, i_data2, n);

  PG_RETURN_FLOAT8(sim);
}

PG_FUNCTION_INFO_V1(cosine_similarity_bytea);

Datum cosine_similarity_bytea(PG_FUNCTION_ARGS) {
  float4 scalar = 0;
  bytea* data1 = PG_GETARG_BYTEA_P(0);
  bytea* data2 = PG_GETARG_BYTEA_P(1);
  float4* v1 = NULL;
  float4* v2 = NULL;
  int size = 0;
  convert_bytea_float4(data1, &v1, &size);
  size = 0;
  convert_bytea_float4(data2, &v2, &size);
  for (int i = 0; i < size; i++) {
    scalar += v1[i] * v2[i];
  }
  PG_RETURN_FLOAT4(scalar);
}

PG_FUNCTION_INFO_V1(vec_minus);

Datum vec_minus(PG_FUNCTION_ARGS) {
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum* i_data1;  // pointer to actual input vector
  Datum* i_data2;  // pointer to actual input vector
  int n = 0;       // dim of input vector

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data1, &n);
  getArray(PG_GETARG_ARRAYTYPE_P(1), &i_data2, &n);

  dvalues = (Datum*)palloc(sizeof(Datum) * n);

  for (int i = 0; i < n; i++) {
    dvalues[i] =
        Float4GetDatum(DatumGetFloat4(i_data1[i]) - DatumGetFloat4(i_data2[i]));
  }

  dims[0] = n;
  lbs[0] = 1;
  get_typlenbyvalalign(FLOAT4OID, &i_typlen, &i_typbyval, &i_typalign);
  v = construct_md_array(dvalues, NULL, 1, dims, lbs, FLOAT4OID, i_typlen,
                         i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);
}

PG_FUNCTION_INFO_V1(vec_minus_bytea);

Datum vec_minus_bytea(PG_FUNCTION_ARGS) {
  float4* i_data1;  // pointer to actual input vector
  float4* i_data2;  // pointer to actual input vector
  int n = 0;        // dim of input vector

  float4* dvalues;
  bytea* output;

  convert_bytea_float4(PG_GETARG_BYTEA_P(0), &i_data1, &n);
  n = 0;
  convert_bytea_float4(PG_GETARG_BYTEA_P(1), &i_data2, &n);
  dvalues = (float4*)palloc(sizeof(float4) * n);

  for (int i = 0; i < n; i++) {
    dvalues[i] = i_data1[i] - i_data2[i];
  }

  convert_float4_bytea(dvalues, &output, n);
  PG_RETURN_BYTEA_P(output);
}

PG_FUNCTION_INFO_V1(vec_plus);

Datum vec_plus(PG_FUNCTION_ARGS) {
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum* i_data1;  // pointer to actual input vector
  Datum* i_data2;  // pointer to actual input vector
  int n = 0;       // dim of input vector

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data1, &n);
  getArray(PG_GETARG_ARRAYTYPE_P(1), &i_data2, &n);

  dvalues = (Datum*)palloc(sizeof(Datum) * n);

  for (int i = 0; i < n; i++) {
    dvalues[i] =
        Float4GetDatum(DatumGetFloat4(i_data1[i]) + DatumGetFloat4(i_data2[i]));
  }

  dims[0] = n;
  lbs[0] = 1;

  get_typlenbyvalalign(FLOAT4OID, &i_typlen, &i_typbyval, &i_typalign);
  v = construct_md_array(dvalues, NULL, 1, dims, lbs, FLOAT4OID, i_typlen,
                         i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);
}

PG_FUNCTION_INFO_V1(vec_plus_bytea);

Datum vec_plus_bytea(PG_FUNCTION_ARGS) {
  float4* i_data1;  // pointer to actual input vector
  float4* i_data2;  // pointer to actual input vector
  int n = 0;        // dim of input vector

  float4* dvalues;
  bytea* output;
  convert_bytea_float4(PG_GETARG_BYTEA_P(0), &i_data1, &n);
  n = 0;
  convert_bytea_float4(PG_GETARG_BYTEA_P(1), &i_data2, &n);
  dvalues = (float4*)palloc(sizeof(float4) * n);

  for (int i = 0; i < n; i++) {
    dvalues[i] = i_data1[i] + i_data2[i];
  }
  convert_float4_bytea(dvalues, &output, n);
  PG_RETURN_BYTEA_P(output);
}

PG_FUNCTION_INFO_V1(vec_normalize);

Datum vec_normalize(PG_FUNCTION_ARGS) {
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;

  Datum* i_data;  // pointer to actual input vector
  int n = 0;      // dim of input vector

  int dims[1];
  int lbs[1];
  Datum* dvalues;
  ArrayType* v;

  float sq_length = 0;
  float length = 0;

  getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data, &n);

  dvalues = (Datum*)palloc(sizeof(Datum) * n);

  // determine square length
  for (int i = 0; i < n; i++) {
    sq_length += DatumGetFloat4(i_data[i]) * DatumGetFloat4(i_data[i]);
  }
  length = sqrt(sq_length);

  // normalize
  for (int i = 0; i < n; i++) {
    dvalues[i] = Float4GetDatum(DatumGetFloat4(i_data[i]) / length);
  }

  dims[0] = n;
  lbs[0] = 1;

  get_typlenbyvalalign(FLOAT4OID, &i_typlen, &i_typbyval, &i_typalign);
  v = construct_md_array(dvalues, NULL, 1, dims, lbs, FLOAT4OID, i_typlen,
                         i_typbyval, i_typalign);

  PG_RETURN_ARRAYTYPE_P(v);
}

PG_FUNCTION_INFO_V1(vec_normalize_bytea);

Datum vec_normalize_bytea(PG_FUNCTION_ARGS) {
  float4* i_data;  // pointer to actual input vector
  int n = 0;       // dim of input vector

  float4* output;
  bytea* v;

  float sq_length = 0;
  float length = 0;

  convert_bytea_float4(PG_GETARG_BYTEA_P(0), &i_data, &n);

  // determine square length
  for (int i = 0; i < n; i++) {
    sq_length += i_data[i] * i_data[i];
  }
  length = sqrt(sq_length);

  // normalize
  output = palloc(n * sizeof(float4));
  for (int i = 0; i < n; i++) {
    output[i] = i_data[i] / length;
  }

  convert_float4_bytea(output, &v, n);
  PG_RETURN_BYTEA_P(v);
}

PG_FUNCTION_INFO_V1(centroid);

Datum centroid(PG_FUNCTION_ARGS) {
  typedef struct {
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    Oid elmtype;
  } elm_info;

  elm_info info;
  int numelems;

  AnyArrayType* arr;

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

  get_typlenbyvalalign(info.elmtype, &info.elmlen, &info.elmbyval,
                       &info.elmalign);
  if (AARR_NDIM(arr) != 2) {
    elog(ERROR, "Input is not a 2-dimensional array");
  }

  vec_size = AARR_DIMS(arr)[1];
  output = palloc(sizeof(float) * vec_size);
  input_size = numelems / vec_size;

  for (int i = 0; i < vec_size; i++) {
    output[i] = 0;
  }

  while (nextelem < numelems) {
    int offset = nextelem++;
    Datum elem;

    elem = array_iter_next(&iter, &fcinfo->isnull, offset, info.elmlen,
                           info.elmbyval, info.elmalign);
    output[offset % vec_size] += DatumGetFloat4(elem) / (float)input_size;
  }

  dvalues = (Datum*)palloc(sizeof(Datum) * vec_size);

  for (int i = 0; i < vec_size; i++) {
    dvalues[i] = Float4GetDatum(output[i]);
  }

  dims[0] = vec_size;
  lbs[0] = 1;
  v = construct_md_array(dvalues, NULL, 1, dims, lbs, info.elmtype, info.elmlen,
                         info.elmbyval, info.elmalign);

  PG_RETURN_ARRAYTYPE_P(v);
}

PG_FUNCTION_INFO_V1(centroid_bytea);

Datum centroid_bytea(PG_FUNCTION_ARGS) {
  // AnyArrayType *v;

  int input_size;
  bytea* v;

  int vec_size = 0;
  float* output;

  Datum* i_data;
  float4** data;

  int n = 0;

  getArray(PG_GETARG_ARRAYTYPE_P(0), &i_data, &n);
  data = palloc(n * sizeof(float4*));
  input_size = n;
  for (int i = 0; i < n; i++) {
    int size = 0;
    convert_bytea_float4(DatumGetByteaP(i_data[i]), &data[i], &size);
    vec_size = size;
  }

  output = palloc(sizeof(float) * vec_size);

  for (int i = 0; i < vec_size; i++) {
    output[i] = 0;
  }

  for (int i = 0; i < input_size; i++) {
    for (int j = 0; j < vec_size; j++) {
      output[j] += data[i][j] / (float)input_size;
    }
  }

  convert_float4_bytea(output, &v, vec_size);
  PG_RETURN_BYTEA_P(v);
}
