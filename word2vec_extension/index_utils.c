#include "index_utils.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/array.h"

void updateTopK(TopK tk, float distance, int id, int k, int maxDist){
  int i;
  for (i = k-1; i >= 0; i--){
    if (tk[i].distance < distance){
      break;
    }
  }
  i++;
  for (int j = k-2; j >= i; j--){
    tk[j+1].distance = tk[j].distance;
    tk[j+1].id = tk[j].id;
  }
  tk[i].distance = distance;
  tk[i].id = id;
}

float squareDistance(float* v1, float* v2, int n){
  float result = 0;
  for (int i = 0; i < n; i++){
    float prod = (v1[i] - v2[i])*(v1[i] - v2[i]);
    result += prod;
  }

  return result;
}

CoarseQuantizer getCoarseQuantizer(int* size){
  char* command;
  int ret;
  int proc;
  CoarseQuantizer result;
  SPI_connect();
  command = "SELECT * FROM coarse_quantization";
  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  *size = proc;
  result = malloc(proc * sizeof(CoarseQuantizerEntry));
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < proc; i++){
      Datum id;
      Datum vector;
      Datum* data;

      Oid eltype;
      int16 typlen;
      bool typbyval;
      char typalign;
      bool *nulls;
      int n = 0;

      ArrayType* vectorAt;

      bool info;
      HeapTuple tuple = tuptable->vals[i];
      id = SPI_getbinval(tuple, tupdesc, 1, &info);
      vector = SPI_getbinval(tuple, tupdesc, 2, &info);
      vectorAt = DatumGetArrayTypeP(vector);
      eltype = ARR_ELEMTYPE(vectorAt);
      get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
      deconstruct_array(vectorAt, eltype, typlen, typbyval, typalign, &data, &nulls, &n);

      result[i].id = DatumGetInt32(id);
      result[i].vector = malloc(n*sizeof(float));
      for (int j=0; j< n; j++){
        result[i].vector[j] = DatumGetFloat4(data[j]);
      }
    }
    SPI_finish();
  }


  return result;
}

Codebook getCodebook(int* positions, int* codesize, char* tableName){
  char command[50];
  int ret;
  int proc;
  Codebook result;
  SPI_connect();
  sprintf(command, "SELECT * FROM %s", tableName);

  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  result = malloc(proc * sizeof(CodebookEntry));
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < proc; i++){

      Datum pos;
      Datum code;
      Datum vector;
      Datum* data;

      Oid eltype;
      int16 typlen;
      bool typbyval;
      char typalign;
      bool *nulls;
      int n = 0;

      ArrayType* vectorAt;

      bool info;
      HeapTuple tuple = tuptable->vals[i];
      pos = SPI_getbinval(tuple, tupdesc, 2, &info);
      code = SPI_getbinval(tuple, tupdesc, 3, &info);
      vector = SPI_getbinval(tuple, tupdesc, 4, &info);
      vectorAt = DatumGetArrayTypeP(vector);
      eltype = ARR_ELEMTYPE(vectorAt);
      get_typlenbyvalalign(eltype, &typlen, &typbyval, &typalign);
      deconstruct_array(vectorAt, eltype, typlen, typbyval, typalign, &data, &nulls, &n);

      (*positions) = fmax((*positions), pos);
      (*codesize) = fmax((*codesize), code);

      result[i].pos = DatumGetInt32(pos);
      result[i].code = DatumGetInt32(code);
      result[i].vector = malloc(n*sizeof(float));
      for (int j=0; j< n; j++){
        result[i].vector[j] = DatumGetFloat4(data[j]);
      }
    }
    SPI_finish();
  }

  *positions += 1;
  *codesize += 1;

  return result;
}

void freeCodebook(Codebook cb, int size){
  for (int i = 0; i < size; i++){
    free(cb[i].vector);
  }
  free(cb);
}
