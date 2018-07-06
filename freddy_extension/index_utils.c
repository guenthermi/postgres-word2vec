#include "index_utils.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "stdlib.h"
#include "time.h"
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

void updateTopKPV(TopKPV tk, float distance, int id, int k, int maxDist, float4* vector, int dim){
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
    memcpy(tk[j+1].vector, tk[j].vector, dim*sizeof(float4));
    //tk[j+1].vector = tk[j].vector;
  }
  tk[i].distance = distance;
  tk[i].id = id;
  memcpy(tk[i].vector, vector, dim*sizeof(float4));
}

void updateTopKWordEntry(char** term, char* word){
  char* cur = word;
  memset(word,0,strlen(word));
  for (int p = 0; term[p]; p++){
    if (term[p+1] == NULL){
      cur += sprintf(cur, "%s", term[p]);
    }else{
      cur += sprintf(cur, "%s ", term[p]);
    }
  }
}

void initTopK(TopK* pTopK, int k, const float maxDist){
  *pTopK = palloc(k*sizeof(TopKEntry));
  for (int i = 0; i < k; i++){
    (*pTopK)[i].distance = maxDist;
    (*pTopK)[i].id = -1;
  }
}

void initTopKs(TopK** pTopKs, float** pMaxDists, int queryVectorsSize, int k, const float maxDist){
  *pTopKs = palloc(queryVectorsSize*sizeof(TopK));
  *pMaxDists = palloc(sizeof(float)*queryVectorsSize);
  for (int i = 0; i < queryVectorsSize; i++){
    initTopK(&((*pTopKs)[i]), k, maxDist);
    (*pMaxDists)[i] = maxDist;
  }
}

void initTopKPV(TopKPV* pTopK, int k, const float maxDist, int dim){
  *pTopK = palloc(k*sizeof(TopKPVEntry));
  for (int i = 0; i < k; i++){
    (*pTopK)[i].distance = maxDist;
    (*pTopK)[i].id = -1;
    (*pTopK)[i].vector = palloc(sizeof(float4)*dim);
  }
}

void initTopKPVs(TopKPV** pTopKs, float** pMaxDists, int queryVectorsSize, int k, const float maxDist, int dim){
  *pTopKs = palloc(queryVectorsSize*sizeof(TopKPV));
  *pMaxDists = palloc(sizeof(float)*queryVectorsSize);
  for (int i = 0; i < queryVectorsSize; i++){
    initTopKPV(&((*pTopKs)[i]), k, maxDist, dim);
    (*pMaxDists)[i] = maxDist;
  }
}

bool inBlacklist(int id, Blacklist* bl){
  if (bl->isValid){
    if (bl->id == id){
      return true;
    }else{
      return inBlacklist(id, bl->next);
    }
  }else{
    return false;
  }
}

void addToBlacklist(int id, Blacklist* bl, Blacklist* emptyBl){
  while (bl->isValid){
    bl = bl->next;
  }
  bl->id = id;
  bl->next = emptyBl;
  bl->isValid = true;
}

void determineCoarseIds(int** pCqIds, int*** pCqTableIds, int** pCqTableIdCounts,
                        int* queryVectorsIndices, int queryVectorsIndicesSize, int queryVectorsSize,
                        float maxDist, CoarseQuantizer cq, int cqSize,
                        float4** queryVectors, int queryDim){
  int* cqIds;
  int** cqTableIds;
  int* cqTableIdCounts;
  float minDist;
  float dist;

  *pCqIds = palloc(queryVectorsSize*sizeof(int));
  cqIds = *pCqIds;

  *pCqTableIds = palloc(sizeof(int*)*cqSize);
  cqTableIds = *pCqTableIds;

  *pCqTableIdCounts = palloc(sizeof(int)*cqSize);
  cqTableIdCounts = *pCqTableIdCounts;

  for (int i = 0; i < cqSize; i++){
    cqTableIds[i] = NULL;
    cqTableIdCounts[i] = 0;
  }

  for (int x = 0; x < queryVectorsIndicesSize; x++){
    int queryIndex = queryVectorsIndices[x];
    int cqId = -1;
    minDist = maxDist;

    for (int j=0; j < cqSize; j++){
      dist = squareDistance(queryVectors[queryIndex], cq[j].vector, queryDim);
      if (dist < minDist){
        cqId = j;
        cqIds[queryIndex] = cqId;
        minDist = dist;
      }
    }
    if (cqTableIdCounts[cqId] == 0){
        cqTableIds[cqId] = palloc(sizeof(int)*queryVectorsIndicesSize);
    }
    cqTableIds[cqId][cqTableIdCounts[cqId]] = queryIndex;
    cqTableIdCounts[cqId] += 1;
  }

}

void postverify(int* queryVectorsIndices, int queryVectorsIndicesSize,
                int k, int pvf, TopKPV* topKPVs, TopK* topKs, float4** queryVectors,
                int queryDim, const float maxDistance){
  for (int x = 0; x < queryVectorsIndicesSize; x++){
    int queryIndex = queryVectorsIndices[x];
    float maxDist = maxDistance;
    float distance;
    for (int j = 0; j < k*pvf; j++){
      // calculate distances
      if (topKPVs[queryIndex][j].id != -1){
        distance = squareDistance(queryVectors[queryIndex], topKPVs[queryIndex][j].vector, queryDim);
        if (distance < maxDist){
          updateTopK(topKs[queryIndex], distance,  topKPVs[queryIndex][j].id, k, maxDist);
          maxDist = topKs[queryIndex][k-1].distance;
        }
      }
    }
  }
}

float squareDistance(float* v1, float* v2, int n){
  float result = 0;
  for (int i = 0; i < n; i++){
    float prod = (v1[i] - v2[i])*(v1[i] - v2[i]);
    result += prod;
  }

  return result;
}

void shuffle(int* input, int* output, int inputSize, int outputSize){
  int i;
  int j;
  int t;
  int* tmp = palloc(sizeof(int)*inputSize);
  srand(time(0));

  for (i = 0; i < inputSize; i++){
    tmp[i] = input[i];
  }
  for (i = 0; i < outputSize; i++)
  {
    j = i + rand() / (RAND_MAX / (inputSize - i) + 1);
    t = tmp[j];
    tmp[j] = tmp[i];
    tmp[i] = t;
  }
  for (i = 0; i < outputSize; i++){
    output[i] = tmp[i];
  }
}

CoarseQuantizer getCoarseQuantizer(int* size){
  char* command;
  int ret;
  int proc;

  float4 *tmp;

  CoarseQuantizer result;
  char* tableNameCQ = palloc(sizeof(char)*100);
  getTableName(COARSE_QUANTIZATION, tableNameCQ, 100);

  SPI_connect();
  command = palloc(100*sizeof(char));
  sprintf(command, "SELECT * FROM %s", tableNameCQ);
  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  *size = proc;
  result = SPI_palloc(proc * sizeof(CoarseQuantizerEntry));
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < proc; i++){
      Datum id;
      Datum vector;

      int n = 0;

      bytea* vectorData;

      bool info;
      HeapTuple tuple = tuptable->vals[i];
      id = SPI_getbinval(tuple, tupdesc, 1, &info);
      vector = SPI_getbinval(tuple, tupdesc, 2, &info);
      vectorData = DatumGetByteaP(vector);

      result[i].id = DatumGetInt32(id);
      tmp = (float4 *) VARDATA(vectorData);
      result[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result[i].vector, tmp, n*sizeof(float4));
    }
    SPI_finish();
  }


  return result;
}

Codebook getCodebook(int* positions, int* codesize, char* tableName){
  char command[100];
  int ret;
  int proc;

  float4* tmp;

  Codebook result;

  bytea* vectorData;

  SPI_connect();
  sprintf(command, "SELECT * FROM %s", tableName);
  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  result = SPI_palloc(proc * sizeof(CodebookEntry));
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < proc; i++){

      Datum pos;
      Datum code;
      Datum vector;

      int n = 0;

      bool info;
      HeapTuple tuple = tuptable->vals[i];
      pos = SPI_getbinval(tuple, tupdesc, 2, &info);
      code = SPI_getbinval(tuple, tupdesc, 3, &info);
      vector = SPI_getbinval(tuple, tupdesc, 4, &info);

      (*positions) = fmax((*positions), pos);
      (*codesize) = fmax((*codesize), code);

      result[i].pos = DatumGetInt32(pos);
      result[i].code = DatumGetInt32(code);

      vectorData = DatumGetByteaP(vector);
      tmp = (float4 *) VARDATA(vectorData);
      result[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result[i].vector, tmp, n*sizeof(float4));
    }
    SPI_finish();
  }

  *positions += 1;
  *codesize += 1;

  return result;
}

CodebookWithCounts getCodebookWithCounts(int* positions, int* codesize, char* tableName){
  char command[50];
  int ret;
  int proc;
  float4* tmp;

  bytea* vectorData;

  CodebookWithCounts result;
  SPI_connect();
  sprintf(command, "SELECT * FROM %s", tableName);

  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  result = SPI_palloc(proc * sizeof(CodebookEntryComplete));
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    int i;
    for (i = 0; i < proc; i++){

      Datum pos;
      Datum code;
      Datum vector;
      Datum count;
      int n = 0;

      bool info;
      HeapTuple tuple = tuptable->vals[i];
      pos = SPI_getbinval(tuple, tupdesc, 2, &info);
      code = SPI_getbinval(tuple, tupdesc, 3, &info);
      vector = SPI_getbinval(tuple, tupdesc, 4, &info);
      count = SPI_getbinval(tuple, tupdesc, 5, &info);

      (*positions) = fmax((*positions), pos);
      (*codesize) = fmax((*codesize), code);

      result[i].pos = DatumGetInt32(pos);
      result[i].code = DatumGetInt32(code);
      vectorData = DatumGetByteaP(vector);
      tmp = (float4*) VARDATA(vectorData);

      result[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result[i].vector, tmp, n*sizeof(float4));
      result[i].count = DatumGetInt32(count);
    }
    SPI_finish();
  }

  *positions += 1;
  *codesize += 1;

  return result;
}

WordVectors getVectors(char* tableName, int* ids, int idsSize){
  char* command;
  char* cur;
  int ret;
  int proc;
  bool info;

  int n = 0;

  WordVectors result;

  result.vectors = palloc(sizeof(float*)*idsSize);
  result.ids = palloc(sizeof(int)*idsSize);

  SPI_connect();

  command = palloc((100 + idsSize*18)*sizeof(char));
  sprintf(command, "SELECT id, vector FROM %s WHERE id IN (", tableName);
  // fill command
  cur = command + strlen(command);
  for (int i = 0; i < idsSize; i++){
    if ( i == idsSize - 1){
        cur += sprintf(cur, "%d", ids[i]);
    }else{
      cur += sprintf(cur, "%d, ", ids[i]);
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
      float4* data;

      int wordId;

      HeapTuple tuple = tuptable->vals[i];
      id = SPI_getbinval(tuple, tupdesc, 1, &info);
      vector = SPI_getbinval(tuple, tupdesc, 2, &info);
      wordId = DatumGetInt32(id);

      convert_bytea_float4(DatumGetByteaP(vector), &data, &n);

      result.vectors[i] = SPI_palloc(sizeof(float)*n);
      result.ids[i] = wordId;
      for (int j = 0; j < n; j++){
        result.vectors[i][j] = data[j];
      }
    }
  }
  SPI_finish();

  return result;
}

void getArray(ArrayType* input, Datum** result, int* n){
  Oid i_eltype;
  int16 i_typlen;
  bool i_typbyval;
  char i_typalign;
  bool *nulls;

  i_eltype = ARR_ELEMTYPE(input);
  get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
  deconstruct_array(input, i_eltype, i_typlen, i_typbyval, i_typalign, result, &nulls, n);
}

void getTableName(tableType type, char* name, int bufferSize){
  char* command;
  int ret;
  int proc;

  const char* function_names[] = {
    "get_vecs_name_original()",
    "get_vecs_name()",
    "get_vecs_name_pq_quantization()",
    "get_vecs_name_codebook()",
    "get_vecs_name_residual_quantization()",
    "get_vecs_name_coarse_quantization()",
    "get_vecs_name_residual_codebook()",
    "get_vecs_name_residual_quantization_complete()",
    "get_vecs_name_ivpq_quantization()"
  };

  SPI_connect();

  command = palloc(100*sizeof(char));
  sprintf(command, "SELECT * FROM %s", function_names[type]);

  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    HeapTuple tuple;
    if (proc != 1){
      elog(ERROR, "Unexpected number of results: %d", proc);
    }
    tuple = tuptable->vals[0];

    snprintf(name, bufferSize, "%s", SPI_getvalue(tuple, tupdesc, 1));
  }
  SPI_finish();
}

void getParameter(parameterType type, int* param){
  char* command;
  int ret;
  int proc;
  bool info;

  const char* function_names[] = {
    "get_pvf()",
    "get_w()"};

  SPI_connect();

  command = palloc(100*sizeof(char));
  sprintf(command, "SELECT * FROM %s", function_names[type]);

  ret = SPI_exec(command, 0);
  proc = SPI_processed;
  if (ret > 0 && SPI_tuptable != NULL){
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    HeapTuple tuple;
    if (proc != 1){
      elog(ERROR, "Unexpected number of results: %d", proc);
    }
    tuple = tuptable->vals[0];
    *param = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &info));
  }
  SPI_finish();
}


// inspired by https://stackoverflow.com/questions/9210528/split-string-with-delimiters-in-c?answertab=oldest#tab-top
typedef struct {
    const char *start;
    size_t len;
} token;

char **split(const char *str, char sep)
{
    char **array;
    unsigned int start = 0, stop, toks = 0, t;
    token *tokens = palloc((strlen(str) + 1) * sizeof(token));
    for (stop = 0; str[stop]; stop++) {
        if (str[stop] == sep) {
            tokens[toks].start = str + start;
            tokens[toks].len = stop - start;
            toks++;
            start = stop + 1;
        }
    }
    /* Mop up the last token */
    tokens[toks].start = str + start;
    tokens[toks].len = stop - start;
    toks++;
    array = palloc((toks + 1) * sizeof(char*));
    for (t = 0; t < toks; t++) {
        /* Calloc makes it nul-terminated */
        char *token = calloc(tokens[t].len + 1, 1);
        memcpy(token, tokens[t].start, tokens[t].len);
        array[t] = token;
    }
    /* Add a sentinel */
    array[t] = NULL;
    return array;
}

void updateCodebook(float** rawVectors, int rawVectorsSize, int subvectorSize, CodebookWithCounts cb, int cbPositions, int cbCodes, int** nearestCentroids, int* countIncs){
  float* minDist = palloc(sizeof(float)*cbPositions);
  float** differences = palloc(cbPositions*cbCodes*sizeof(float*));
  float* nearestCentroidRaw = NULL;

  for (int i = 0; i < (cbPositions*cbCodes); i++){
    differences[i] = palloc(subvectorSize*sizeof(float));
    for (int j = 0; j < subvectorSize; j++){
      differences[i][j] = 0;
    }
    countIncs[i] = 0;
  }

  for (int i = 0; i < rawVectorsSize; i++){
    nearestCentroids[i] = palloc(sizeof(int)*cbPositions);
    for (int j=0; j< cbPositions; j++){
      minDist[j] = 100; // sufficient high value
    }
    for (int j=0; j< cbPositions*cbCodes; j++){
      int pos = cb[j].pos;
      int code = cb[j].code;
      float* vector = cb[j].vector;
      float dist = squareDistance(rawVectors[i]+(pos*subvectorSize), vector, subvectorSize);
      if (dist < minDist[pos]){
        nearestCentroids[i][pos] = code;
        minDist[pos] = dist;
        nearestCentroidRaw = vector;
      }
    }
    for (int j=0; j< cbPositions; j++){
      int code = nearestCentroids[i][j];
      countIncs[j*cbCodes + code] += 1;
      for (int k=0; k < subvectorSize; k++){
        differences[j*cbCodes + code][k] += nearestCentroidRaw[k];
      }
    }
  }

  // recalculate codebook
  for (int i = 0; i < cbPositions*cbCodes; i++){
    cb[i].count += countIncs[cb[i].pos*cbCodes + cb[i].code];
    for (int j=0; j < subvectorSize; j++){
        cb[i].vector[j] += (1.0 / cb[i].count) * differences[cb[i].pos + cb[i].code][j];
    }
  }
}

void updateCodebookRelation(CodebookWithCounts cb, int cbPositions, int cbCodes, char* tableNameCodebook, int* countIncs, int subvectorSize){
  char* command;
  char* cur;
  int ret;

  for (int i=0; i < cbPositions*cbCodes; i++){
    if (countIncs[cb[i].pos*cbCodes + cb[i].code] > 0){
      // update codebook entry
      command = palloc(sizeof(char)*(subvectorSize*16+6+6+100));
      cur = command;
      cur += sprintf(cur, "UPDATE %s SET (vector, count) = (vec_to_bytea('{", tableNameCodebook);
      for (int j = 0; j < subvectorSize; j++){
        if (j < subvectorSize-1){
          cur += sprintf(cur, "%f, ", cb[i].vector[j]);
        }else{
          cur += sprintf(cur, "%f", cb[i].vector[j]);
        }
      }
      cur += sprintf(cur, "}'::float4[]), '%d')", cb[i].count);
      cur += sprintf(cur, " WHERE (pos = %d) AND (code = %d)", cb[i].pos, cb[i].code);
      SPI_connect();
      ret = SPI_exec(command, 0);
      if (ret > 0){
        SPI_finish();
      }
      pfree(command);
    }
  }
}
void updateProductQuantizationRelation(int** nearestCentroids, char** tokens, int cbPositions, CodebookWithCounts cb, char* pqQuantizationTable, int rawVectorsSize, int* cqQuantizations){
  char* command;
  char* cur;
  int ret;
  const char* schema_pq_quantization = "(id, word, vector)";
  const char* schema_fine_quantization = "(id, coarse_id, word, vector)";

  for (int i=0; i < rawVectorsSize; i++){
    command = palloc(sizeof(char)*(100 + cbPositions*6 + 200));
    cur = command;
    if (cqQuantizations == NULL){
      cur += sprintf(cur, "INSERT INTO %s %s VALUES ((SELECT max(id) + 1 FROM %s), ", pqQuantizationTable, schema_pq_quantization, pqQuantizationTable);
      cur += sprintf(cur, "'%s', vec_to_bytea('{", tokens[i]);
    }else{
      cur += sprintf(cur, "INSERT INTO %s %s VALUES ((SELECT max(id) + 1 FROM %s), ", pqQuantizationTable, schema_fine_quantization, pqQuantizationTable);
      cur += sprintf(cur, "%d, '%s', vec_to_bytea('{", cqQuantizations[i], tokens[i]);
    }
    for (int j = 0; j < cbPositions; j++){
      if (j < (cbPositions - 1)){
        cur += sprintf(cur, "%d,", nearestCentroids[i][j]);
      }else{
        cur += sprintf(cur, "%d", nearestCentroids[i][j]);
      }
    }
    cur += sprintf(cur, "}'::int2[])");
    cur += sprintf(cur, ")");
    SPI_connect();
    ret = SPI_exec(command, 0);
    if (ret > 0){
      SPI_finish();
    }
    pfree(command);
  }
}

void updateWordVectorsRelation(char* tableName, char** tokens, float** rawVectors, int rawVectorsSize, int vectorSize){
  char* command;
  char* cur;
  int ret;
  for (int i=0; i < rawVectorsSize; i++){
    command = palloc(sizeof(char)*(100 + vectorSize*10 + 200));
    cur = command;
    cur += sprintf(cur, "INSERT INTO %s (id, word, vector) VALUES ((SELECT max(id) + 1 FROM %s), '%s', vec_to_bytea('{", tableName, tableName, tokens[i]);
    for (int j = 0; j < vectorSize; j++){
      if (j < (vectorSize - 1)){
        cur += sprintf(cur, "%f,", rawVectors[i][j]);
      }else{
        cur += sprintf(cur, "%f", rawVectors[i][j]);
      }
    }
    cur += sprintf(cur, "}'::float4[])");
    cur += sprintf(cur, ")");
    SPI_connect();
    ret = SPI_exec(command, 0);
    if (ret > 0){
      SPI_finish();
    }

    pfree(command);
  }
}

int compare (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

void convert_bytea_int32(bytea* bstring, int32** output, int* size){
  int32 *ptr = (int32 *) VARDATA(bstring);
  if (*size == 0){ // if size value is given it is assumed that memory is already allocated
    *output = palloc((VARSIZE(bstring) - VARHDRSZ));
    *size = (VARSIZE(bstring) - VARHDRSZ) / sizeof(int32);
  }
  memcpy(*output, ptr, (*size)*sizeof(int32));
}

void convert_bytea_int16(bytea* bstring, int16** output, int* size){
  int16 *ptr = (int16 *) VARDATA(bstring);
  if (*size == 0){ // if size value is given it is assumed that memory is already allocated
    *output = palloc((VARSIZE(bstring) - VARHDRSZ));
    *size = (VARSIZE(bstring) - VARHDRSZ) / sizeof(int16);
  }
  memcpy(*output, ptr, (*size)*sizeof(int16));
}

void convert_bytea_float4(bytea* bstring, float4** output, int* size){
  float4 *ptr = (float4 *) VARDATA(bstring);
  if (*size == 0){ // if size value is given it is assumed that memory is already allocated
    *output = palloc((VARSIZE(bstring) - VARHDRSZ));
    *size = (VARSIZE(bstring) - VARHDRSZ) / sizeof(float4);
  }
  memcpy(*output, ptr, (*size)*sizeof(float4));
}

void convert_float4_bytea(float4* input, bytea** output, int size){
  *output = (text *) palloc(size*sizeof(float4) + VARHDRSZ);
  SET_VARSIZE(*output, VARHDRSZ + size*sizeof(float4));
  memcpy(VARDATA(*output), input, size*sizeof(float4));
}

void convert_int32_bytea(int32* input, bytea** output, int size){
  *output = (text *) palloc(size*sizeof(int32) + VARHDRSZ);
  SET_VARSIZE(*output, VARHDRSZ + size*sizeof(int32));
  memcpy(VARDATA(*output), input, size*sizeof(int32));
}

void convert_int16_bytea(int16* input, bytea** output, int size){
  *output = (text *) palloc(size*sizeof(int16) + VARHDRSZ);
  SET_VARSIZE(*output, VARHDRSZ + size*sizeof(int16));
  memcpy(VARDATA(*output), input, size*sizeof(int16));
}
