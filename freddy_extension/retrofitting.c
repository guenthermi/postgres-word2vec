// clang-format off

#include "retrofitting.h"

#include "postgres.h"
#include "executor/spi.h"
#include "float.h"
#include "index_utils.h"
#include "math.h"
#include "stdlib.h"
#include "string.h"
#include "utils/builtins.h"
#include "utils/palloc.h"
#include "hashmap.h"

#define JSMN_HEADER
#include "jsmn.h"

// clang-format on

int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
    if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
        strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
        return 0;
    }
    return -1;
}

jsmntok_t* readJsonFile(const char* path, char** json, int* r) {
    FILE* fp;
    long fsize, read_size;
    jsmntok_t* t = NULL;
    jsmn_parser p;

    fp = fopen(path, "r");
    if (fp == NULL) {
        return t;
    }
    fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    rewind(fp);

    *json = palloc0(fsize + 1);
    read_size = fread(*json, 1, fsize, fp);
    if (read_size != fsize){
      elog(INFO, "Warning: read size != fsize");
    }
    fclose(fp);

    jsmn_init(&p);

    *r = jsmn_parse(&p, *json, strlen(*json), NULL, 0);
    t = palloc(*r * sizeof(*t));

    jsmn_init(&p);
    *r = jsmn_parse(&p, *json, strlen(*json), t, *r);
    return t;
}

char* remove_escapes(const char* s){
  char* new_str = palloc0(sizeof(char)*(strlen(s)+1));
  bool backslash = false;
  int j = 0;
  for (int i = 0; i < strlen(s); i++){
      if (backslash) {
        backslash = false;
        switch(s[i]) {
          case 'u':
          case 'n':
          new_str[j++] = '\\';
        }
        new_str[j++] = s[i];
      } else {
        if (s[i] == '\\'){
          backslash = true;
        } else {
          new_str[j++] = s[i];
        }
      }
  }
  return new_str;
}

char* sprintf_json(const char* json, jsmntok_t* t) {
    char* s = palloc0(t->end - t->start + 1);
    char* substr1 = palloc0(6);
    sprintf(s, "%.*s", t->end - t->start, json + t->start);
    memcpy(substr1, s, 5);
    return remove_escapes(s);
}

ColumnData* getColumnData(const char* json, jsmntok_t* t, int k, int* columnDataSize) {
    static ColumnData ret[10];
    int index;

    if (t[k].type != JSMN_OBJECT) {
        return ret;
    }

    *columnDataSize = t[k].size;

    for (int i = 0; i < t[k].size; i++) {
        index = k + i * 6 + 1;

        if (t[index].type == JSMN_STRING) {
            ret[i].name = sprintf_json(json, &t[index]);
        }
        if (t[index + 1].type == JSMN_OBJECT) {
            if (jsoneq(json, &t[index + 2], "mean") == 0) {
                ret[i].mean = sprintf_json(json, &t[index + 3]);
            }
            if (jsoneq(json, &t[index + 4], "size") == 0) {
                ret[i].size = strtol(sprintf_json(json, &t[index + 5]), NULL, 10);
            }
        }
    }
    return ret;
}

CardinalityData* getCardinalities(const char* json, jsmntok_t* t, int k, int size, int* count) {
    CardinalityData* ret = palloc(sizeof(CardinalityData) * size);
    int index;

    if (t[k].type != JSMN_OBJECT) {
        return ret;
    }

    *count = t[k].size * 2;

    for (int i = 0; i < t[k].size; i++) {
        index = k + i * 2 + 1;
        if (t[index].type == JSMN_STRING) {
            (ret + i)->value = sprintf_json(json, &t[index]);
        }
        if (t[index + 1].type == JSMN_PRIMITIVE) {
            (ret + i)->cardinality = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        }
    }
    return ret;
}

RelationColumnData* getRelationColumnData(const char* json, jsmntok_t* t, int k, int* count) {
    int cardinalities = 0;
    RelationColumnData* ret = palloc(sizeof(RelationColumnData));

    if (t[k].type != JSMN_OBJECT) {
        return ret;
    }
    if (jsoneq(json, &t[k + 1], "name") == 0) {
        ret->name = sprintf_json(json, &t[k + 2]);
    }
    if (jsoneq(json, &t[k + 3], "centroid") == 0) {
        ret->centroid = sprintf_json(json, &t[k + 4]);
    }
    if (jsoneq(json, &t[k + 5], "size") == 0) {
        ret->size = strtol(sprintf_json(json, &t[k + 6]), NULL, 10);
    }
    if (jsoneq(json, &t[k + 7], "cardinalities") == 0) {
        ret->cardinalities = getCardinalities(json, t, k + 8, ret->size, &cardinalities);
    }

    *count = 8 + cardinalities;
    return ret;
}

RelationDataObj* getRelationData(const char* json, jsmntok_t* t, int k, int* relationDataSize) {
    RelationDataObj* ret = palloc0(sizeof(RelationDataObj) * t[k].size);
    int index = k;
    int countCol1 = 0;
    int countCol2 = 0;

    if (t[k].type != JSMN_OBJECT) {
        return ret;
    }

    *relationDataSize = t[k].size;

    for (int i = 0; i < t[k].size; i++) {
        index++;
        if (t[index].type == JSMN_STRING) {
            ret[i].relation_name = sprintf_json(json, &t[index]);
        }
        if (t[index + 1].type == JSMN_OBJECT) {
            if (jsoneq(json, &t[index + 2], "name") == 0) {
                ret[i].name = sprintf_json(json, &t[index + 3]);
            }
            if (jsoneq(json, &t[index + 4], "col1") == 0) {
                ret[i].col1 = getRelationColumnData(json, t, index + 5, &countCol1);
            }
            if (jsoneq(json, &t[index + countCol1 + 6], "col2") == 0) {
                ret[i].col2 = getRelationColumnData(json, t, index + countCol1 + 7, &countCol2);
            }
            if (jsoneq(json, &t[index + countCol1 + countCol2 + 8], "max_r") == 0) {
                ret[i].max_r = strtol(sprintf_json(json, &t[index + countCol1 + countCol2 + 9]), NULL, 10);
            }
            if (jsoneq(json, &t[index + countCol1 + countCol2 + 10], "max_c") == 0) {
                ret[i].max_c = strtol(sprintf_json(json, &t[index + countCol1 + countCol2 + 11]), NULL, 10);
            }
        }
        index += countCol1 + countCol2 + 11;
    }
    return ret;
}

RelNumData* getRelNumData(const char* json, jsmntok_t* t, int k, int* relNumDataSize) {
    static RelNumData ret[50000];
    int index;

    if (t[k].type != JSMN_OBJECT) {
        return ret;
    }

    *relNumDataSize = t[k].size;

    for (int i = 0; i < t[k].size; i++) {
        index = k + i * 2 + 1;

        if (t[index].type == JSMN_STRING) {
            ret[i].rel = sprintf_json(json, &t[index]);
        }
        if (t[index + 1].type == JSMN_PRIMITIVE) {
            ret[i].value = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        }
    }
    return ret;
}

DeltaElem* getDeltaElems(const char* json, jsmntok_t* t, int k, int* elemSize) {
    DeltaElem* ret = palloc0(t[k].size * sizeof(DeltaElem));
    int index;

    if (t[k].type != JSMN_OBJECT) {
        return ret;
    }

    *elemSize = t[k].size * 2;

    for (int i = 0; i < t[k].size; i++) {
        index = k + i * 2 + 1;

        if (t[index].type == JSMN_STRING) {
            ret[i].name = sprintf_json(json, &t[index]);
        }
        if (t[index + 1].type == JSMN_STRING) {
            ret[i].value = sprintf_json(json, &t[index + 1]);
        }
    }

    return ret;
}

char** getStringElems(const char* json, jsmntok_t* t, int k, int* elemSize) {
  char** ret = palloc0(t[k].size*sizeof(char*));
  int index;


  if (t[k].type != JSMN_ARRAY) {
    return ret;
  }

  *elemSize = t[k].size;

  for (int i = 0; i < t[k].size; i++) {
    index = k + i + 1;
    if (t[index].type == JSMN_STRING) {
      ret[i] = sprintf_json(json, &t[index]);
    }
  }
  return ret;
}

DeltaCat* getDeltaCats(const char* json, jsmntok_t* t, int maxTok, int* count, int* lastIndex) {
    DeltaCat* ret = palloc0(maxTok * sizeof(DeltaCat));
    int index = 1;
    int elemSize = 0;
    int infElemSize = 0;
    int changedElemSize = 0;
    *count = 0;

    for (int i = 0; index < maxTok; i++) {
        if (t[index + 1].type == JSMN_OBJECT) {
            if (jsoneq(json, &t[index + 2], "name") == 0) {
                (ret + i)->name = sprintf_json(json, &t[index + 3]);
            }
            if (jsoneq(json, &t[index + 4], "type") == 0) {
                (ret + i)->type = sprintf_json(json, &t[index + 5]);
            }
            if (jsoneq(json, &t[index + 6], "elements") == 0) {
                (ret + i)->elements = getDeltaElems(json, t, index + 7, &elemSize);
                (ret + i)->elementCount = elemSize / 2;
            }
            if (jsoneq(json, &t[index + 8 + elemSize], "query") == 0) {
                (ret + i)->query = sprintf_json(json, &t[index + 9 + elemSize]);
            }
            if (jsoneq(json, &t[index + 10 + elemSize], "inferred_elements") == 0) {
                (ret + i)->inferredElements = getDeltaElems(json, t, index + 11 + elemSize, &infElemSize);
                (ret + i)->inferredElementCount = infElemSize / 2;
            }
            if (jsoneq(json, &t[index + 12 + elemSize + infElemSize], "changed") == 0) {
                (ret + i)->changedElements = getStringElems(json, t, index + 13 + elemSize + infElemSize, &changedElemSize);
                (ret + i)->changedElementsCount = changedElemSize;
            }
            *count += 1;
        }
        if (t[index + 1].type == JSMN_ARRAY) break;
        index += 14 + elemSize + infElemSize + changedElemSize;
    }
    *lastIndex = index;
    return ret;
}

DeltaRel* getDeltaRels(const char* json, jsmntok_t* t, int maxTok, int* count, int start) {
    DeltaRel* ret = palloc0(maxTok * sizeof(DeltaRel));
    int size;
    int index = start;
    int elemSize = 0;
    int changedElemSize = 0;
    *count = 0;

    for (int i = 0; index < maxTok; i++) {
        if (t[index + 1].type == JSMN_ARRAY) {
            (ret + i)->relation = sprintf_json(json, &t[index]);
            (ret + i)->groupCount = 0;
            index += 2;

            size = t[index - 1].size;
            for (int j = 0; j < size; j++) {
                (ret + i)->groups = prealloc((ret + i)->groups, sizeof(DeltaRelElem) * ((ret + i)->groupCount + 1));
                if (jsoneq(json, &t[index + 1], "name") == 0) {
                    (ret + i)->groups[(ret + i)->groupCount].name = sprintf_json(json, &t[index + 2]);
                }
                if (jsoneq(json, &t[index + 3], "type") == 0) {
                    (ret + i)->groups[(ret + i)->groupCount].type = sprintf_json(json, &t[index + 4]);
                }
                if (jsoneq(json, &t[index + 5], "elements") == 0) {
                    (ret + i)->groups[(ret + i)->groupCount].elements = getDeltaElems(json, t, index + 6, &elemSize);
                    (ret + i)->groups[(ret + i)->groupCount].elementCount = elemSize / 2;
                }
                if (jsoneq(json, &t[index + 7 + elemSize], "changed") == 0) {
                  (ret + i)->groups[(ret + i)->groupCount].changedElements = getStringElems(json, t, index + 8 + elemSize, &changedElemSize);
                  (ret + i)->groups[(ret + i)->groupCount].changedElementsCount = changedElemSize;
                }
                (ret + i)->groupCount += 1;
                index += 8 + elemSize + changedElemSize + 1;
            }
            *count += 1;
        }
    }
    return ret;
}

char* escape(char* str) {
    int len = strlen(str);

    for (int i = 0; i < len; i++) {
        if (str[i] == '\'') {
            char *substr1, *substr2;
            substr1 = palloc0(len + 1);
            memcpy(substr1, str, i);

            substr2 = palloc0(len - i);
            memcpy(substr2, &str[i + 1], len - i - 1);
            substr2[len - i - 1] = '\0';

            sprintf(substr1, "%s''%s", substr1, escape(substr2));
//            pfree(str);           // TODO: free (pfree / SPI_pfree)
            str = substr1;
            break;
        }
    }
    return str;
}

void clearStats() {
    char* command = palloc0(100);
    char tables[5][20] = {"cardinality_stats", "column_stats", "rel_num_stats", "relation_stats", "rel_col_stats"};

    for (int i = 0; i < 5; i++) {
        sprintf(command, "TRUNCATE %s CASCADE", *(tables + i));

        SPI_connect();
        SPI_exec(command, 0);
        SPI_finish();
    }
    pfree(command);
}

void insertColumnStatistics(ColumnData* columnData, int columnCount) {
    char* command;
    char* cur;

    for (int i = 0; i < columnCount; i++) {
        command = palloc(100
                         + strlen((columnData + i)->name)
                         + strlen((columnData + i)->mean)
                         + 10);
        cur = command;

        cur += sprintf(
                cur, "INSERT INTO column_stats (name, mean, size) VALUES ('%s', '%s', %d) "
                     "ON CONFLICT (name) DO UPDATE SET (mean, size) = (EXCLUDED.mean, EXCLUDED.size)",
                (columnData + i)->name, (columnData + i)->mean, (columnData + i)->size);
        SPI_connect();
        SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

void updateCardinalityStatistics(struct CardinalityData* cardinalityData, int count, char* relColId) {
    char* command;
    char* cur;

    for (int i = 0; i < count; i++) {
        command = palloc(100
                         + strlen((cardinalityData + i)->value)
                         + 10);
        cur = command;
        cur += sprintf(
                cur, "INSERT INTO cardinality_stats (value, cardinality, col_id) VALUES ('%s', %d, %s) ",
                escape((cardinalityData + i)->value), (cardinalityData + i)->cardinality, relColId);
        SPI_connect();
        SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

char* updateRelColStatistics(struct RelationColumnData* relationColumnData) {
    char* command;
    char* cur;
    char* id = palloc(100);
    char* id_tmp;
    ResultInfo rInfo;

    command = palloc(100
                     + strlen(relationColumnData->name)
                     + strlen(relationColumnData->centroid)
                     + 10);
    cur = command;
    cur += sprintf(
            cur, "INSERT INTO rel_col_stats (name, centroid, size) VALUES ('%s', '%s', %d) "
                 "RETURNING id",
            escape(relationColumnData->name), escape(relationColumnData->centroid), relationColumnData->size);
    SPI_connect();
    rInfo.ret = SPI_exec(command, 0);
    rInfo.proc = SPI_processed;
    if (rInfo.ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable* tuptable = SPI_tuptable;
        HeapTuple tuple = tuptable->vals[0];
        id_tmp = SPI_getvalue(tuple, tupdesc, 1);
        snprintf(id, strlen(id_tmp)+1,"%s", id_tmp);
        SPI_finish();
    }
    if (id) {
        updateCardinalityStatistics(relationColumnData->cardinalities, relationColumnData->size, id);
    }
    pfree(command);
    return id;
}

void insertRelationStatistics(struct RelationDataObj* relationData, int count) {
    char* col1;
    char* col2;
    char* command;
    char* cur;

    for (int i = 0; i < count; i++) {
        col1 = updateRelColStatistics((relationData + i)->col1);
        col2 = updateRelColStatistics((relationData + i)->col2);

        command = palloc(300
                         + strlen((relationData + i)->relation_name)
                         + strlen((relationData + i)->name)
                         + 20);
        cur = command;
        cur += sprintf(
                cur, "INSERT INTO relation_stats (relation_name, name, col1, col2, max_r, max_c) VALUES ('%s', '%s', %s, %s, %d, %d) "
                     "ON CONFLICT (relation_name) DO UPDATE SET (name, col1, col2, max_r, max_c) = (EXCLUDED.name, EXCLUDED.col1, EXCLUDED.col2, EXCLUDED.max_r, EXCLUDED.max_c)",
                escape((relationData + i)->relation_name), escape((relationData + i)->name), col1, col2, (relationData + i)->max_r, (relationData + i)->max_c);
        SPI_connect();
        SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

void insertRelNumDataStatistics(RelNumData* relNumData, int relCount) {
    char* command;
    char* cur;

    for (int i = 0; i < relCount; i++) {
        command = palloc(100
                         + strlen((relNumData + i)->rel)
                         + 10);
        cur = command;
        cur += sprintf(
                cur, "INSERT INTO rel_num_stats (rel, value) VALUES ('%s', %d) "
                     "ON CONFLICT (rel) DO UPDATE SET value = EXCLUDED.value",
                escape((relNumData + i)->rel), (relNumData + i)->value);
        SPI_connect();
        SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

RetroConfig* getRetroConfig(const char* json, jsmntok_t* t, int* size) {
    RetroConfig* ret = palloc0(sizeof(RetroConfig));
    int index = 1;

    *size = t[0].size;
    for (int i = 0; i < t[0].size; i++) {

        if (jsoneq(json, &t[index], "WE_ORIGINAL_TABLE_NAME") == 0) {
            ret->weOriginalTableName = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "RETRO_TABLE_CONFS") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->retroTableConfs = palloc0(t[index + 1].size * sizeof(char*));
                for (int j = 0; j < t[index + 1].size; j++) {
                    ret->retroTableConfs[j] = sprintf_json(json, &t[index + j + 2]);
                }
            }
            index += t[index + 1].size;
        } else if (jsoneq(json, &t[index], "SCHEMA_GRAPH_PATH") == 0) {
            ret->schemaGraphPath = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "SCHEMA_JSON_GRAPH_PATH") == 0) {
            ret->schemaJsonGraphPath = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "TABLE_BLACKLIST") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->tableBlacklist = palloc0(t[index + 1].size * sizeof(char*));
                for (int j = 0; j < t[index + 1].size; j++) {
                    ret->tableBlacklist[j] = sprintf_json(json, &t[index + j + 2]);
                }
            }
            index += t[index + 1].size;
        } else if (jsoneq(json, &t[index], "COLUMN_BLACKLIST") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->columnBlacklist = palloc0(t[index + 1].size * sizeof(char*));
                for (int j = 0; j < t[index + 1].size; j++) {
                    ret->columnBlacklist[j] = sprintf_json(json, &t[index + j + 2]);
                }
            }
            index += t[index + 1].size;
        } else if (jsoneq(json, &t[index], "RELATION_BLACKLIST") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->relationBlacklist = palloc0(t[index + 1].size * sizeof(char*));
                for (int j = 0; j < t[index + 1].size; j++) {
                    ret->relationBlacklist[j] = sprintf_json(json, &t[index + j + 2]);
                }
            }
            index += t[index + 1].size;
        } else if (jsoneq(json, &t[index], "OUTPUT_FOLDER") == 0) {
            ret->outputFolder = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "GROUPS_FILE_NAME") == 0) {
            ret->groupsFileName = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "ITERATIONS") == 0) {
            ret->iterations = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        } else if (jsoneq(json, &t[index], "RETRO_VECS_FILE_NAME") == 0) {
            ret->retroVecsFileName = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "TOKENIZATION") == 0) {
            ret->tokenization = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "ALPHA") == 0) {
            ret->alpha = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        } else if (jsoneq(json, &t[index], "BETA") == 0) {
            ret->beta = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        } else if (jsoneq(json, &t[index], "GAMMA") == 0) {
            ret->gamma = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        } else if (jsoneq(json, &t[index], "DELTA") == 0) {
            ret->delta = strtol(sprintf_json(json, &t[index + 1]), NULL, 10);
        }
        index += 2;
    }
    return ret;
}

int wordVecCompare(const void *a, const void *b, void *udata) {
    const struct WordVec *wva = a;
    const struct WordVec *wvb = b;
    return strcmp(wva->word, wvb->word);
}

uint64_t wordVecHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct WordVec *wv = item;
    return hashmap_murmur(wv->word, strlen(wv->word), seed0, seed1);
}

int retroExportCompare(const void *a, const void *b, void *udata) {
    const struct RetroExport *wva = a;
    const struct RetroExport *wvb = b;
    return strcmp(wva->word, wvb->word);
}

uint64_t retroExportHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct RetroExport *wv = item;
    return hashmap_murmur(wv->word, strlen(wv->word), seed0, seed1);
}

int colMeanCompare(const void *a, const void *b, void *udata) {
    const struct ColMean *ca = a;
    const struct ColMean *cb = b;
    return strcmp(ca->word, cb->word);
}

uint64_t colMeanHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct ColMean *ci = item;
    return hashmap_murmur(ci->word, strlen(ci->word), seed0, seed1);
}

int CardinalityCountCompare(const void *a, const void *b, void *udata) {
    const struct CardinalityCount *sca = a;
    const struct CardinalityCount *scb = b;
    if (!(sca->colId == scb->colId)) {
      return -1;
    }
    return strcmp(sca->word, scb->word);
}

uint64_t CardinalityCountHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct CardinalityCount *sc = item;
    char* s = palloc0(strlen(sc->word)+20);
    uint64_t res;
    sprintf(s, "%s%d", sc->word, sc->colId);
    res = hashmap_murmur(s, strlen(s), seed0, seed1);
    pfree(s);
    return res;
}

int RelNumEntryCompare(const void *a, const void *b, void *udata) {
    const struct RelNumEntry *rea = a;
    const struct RelNumEntry *reb = b;
    return strcmp(rea->word, reb->word);
}

uint64_t RelNumEntryHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct RelNumEntry *re = item;
    return hashmap_murmur(re->word, strlen(re->word), seed0, seed1);
}

int RelColEntryCompare(const void *a, const void *b, void *udata) {
    const struct RelColEntry *rca = a;
    const struct RelColEntry *rcb = b;
    return (rca->id - rcb->id);
}

uint64_t RelColEntryHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct RelColEntry *re = item;
    return re->id;
}

int DeltaWordCompare(const void *a, const void *b, void *udata) {
    const struct DeltaWord *dwa = a;
    const struct DeltaWord *dwb = b;
    return strcmp(dwa->word, dwb->word);
}

uint64_t DeltaWordHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct DeltaWord *dw = item;
    return hashmap_murmur(dw->word, strlen(dw->word), seed0, seed1);
}

int RelationStatsCompare(const void *a, const void *b, void *udata) {
    const struct RelationStats *rsa = a;
    const struct RelationStats *rsb = b;
    return strcmp(rsa->name, rsb->name);
}

uint64_t RelationStatsHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct RelationStats *rs = item;
    return hashmap_murmur(rs->name, strlen(rs->name), seed0, seed1);
}

struct hashmap* getWordVecs(char* tableName, int* dim, bool ignore_terms_with_spaces) {
    char* command;
    ResultInfo rInfo;
    float4* tmp;
    struct hashmap* result = NULL;
    WordVec* wv;
    int tmpDim = 0;
    void* el = (void*)1;

    if (SPI_connect() == SPI_OK_CONNECT) {
        command = palloc0(100 + strlen(tableName));
        sprintf(command, "SELECT * FROM %s", tableName);
        rInfo.ret = SPI_execute(command, true, 0);
        rInfo.proc = SPI_processed;


        if (rInfo.ret == SPI_OK_SELECT && SPI_tuptable != NULL) {
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            SPITupleTable *tuptable = SPI_tuptable;

            hashmap_set_allocator(SPI_palloc, SPI_pfree);
            result = hashmap_new(sizeof(WordVec), rInfo.proc, 0, 0, wordVecHash, wordVecCompare, NULL);

            for (int i = 0; i < rInfo.proc; i++) {
                Datum id;
                char* word;
                Datum vector;

                int n = 0;

                bytea *vectorData;

                HeapTuple tuple = tuptable->vals[i];
                id = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
                word = (char*) SPI_getvalue(tuple, tupdesc, 2);
                vector = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
                vectorData = DatumGetByteaP(vector);

                if (ignore_terms_with_spaces) {
                    if (word[0] == '_' || word[strlen(word) - 1] == '_') continue;
                }
                wv = SPI_palloc(sizeof(WordVec));

                wv->id = DatumGetInt32(id);
                wv->word = SPI_palloc(strlen(word) + 1);
                snprintf(wv->word, strlen(word) + 1, "%s", word);
                wv->word[strlen(word)] = '\0';
                tmp = (float4 *) VARDATA(vectorData);
                wv->vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
                n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
                memcpy(wv->vector, tmp, n * sizeof(float4));
                wv->dim = n;

                if (tmpDim == 0) {
                    tmpDim = n;
                }

                el = hashmap_set(result, wv);
                if (!el && hashmap_oom(result)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("hash map out of memory")));
                }
            }
        }
        SPI_finish();
    } else {
        elog(WARNING, "WordVecs request failed!");
    }
    hashmap_set_allocator(palloc, pfree);
    if (dim) {
        *dim = tmpDim;
    }
    return result;
}

WordVec* getWordVec(char* tableName, char* word) {
    char* command;
    ResultInfo rInfo;
    float4* tmp;
    WordVec* result = NULL;

    SPI_connect();
    command = palloc0(100);
    sprintf(command, "SELECT * FROM %s WHERE word = '%s'", tableName, escape(word));
    rInfo.ret = SPI_exec(command, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret > 0 && rInfo.proc > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        Datum id;
        char* word;
        Datum vector;
        int n = 0;
        bytea* vectorData;

        HeapTuple tuple = tuptable->vals[0];
        id = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
        word = SPI_getvalue(tuple, tupdesc, 2);
        vector = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
        vectorData = DatumGetByteaP(vector);

        result = SPI_palloc(sizeof(WordVec));
        result->id = DatumGetInt32(id);
        result->word = SPI_palloc(strlen(word) + 1);
        snprintf(result->word, strlen(word) + 1, "%s", word);
        result->word[strlen(word)] = '\0';
        tmp = (float4 *) VARDATA(vectorData);
        result->vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
        n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
        memcpy(result->vector, tmp, n * sizeof(float4));
        result->dim = n;
    }
    SPI_finish();
    return result;
}

int getIntFromDB(char* query) {
    ResultInfo rInfo;
    int result = -1;

    SPI_connect();
    rInfo.ret = SPI_exec(query, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret > 0 && rInfo.proc > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        HeapTuple tuple = tuptable->vals[0];
        result = strtol(SPI_getvalue(tuple, tupdesc, 1), NULL, 10);
    }
    SPI_finish();
    return result;
}

char* getJoinRelFromDB(char* table, char* foreign_table) {
    char* command;
    ResultInfo rInfo;
    char* result = NULL;

    SPI_connect();
    command = palloc0(1000);
    sprintf(command, "SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name "
                     "FROM information_schema.table_constraints AS tc "
                     "JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
                     "JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
                     "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '%s' AND ccu.table_name = '%s'",
                     table, foreign_table);
    rInfo.ret = SPI_execute(command, true, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret == SPI_OK_SELECT && rInfo.proc > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;

        char* table;
        char* col;
        char* fTable;
        char* fCol;
        int length;

        HeapTuple tuple = tuptable->vals[0];
        table = SPI_getvalue(tuple, tupdesc, 1);
        col = SPI_getvalue(tuple, tupdesc, 2);
        fTable = SPI_getvalue(tuple, tupdesc, 3);
        fCol = SPI_getvalue(tuple, tupdesc, 4);

        length = strlen(table) + strlen(col) + strlen(fTable) + strlen(fCol) + 5 + 1;
        result = SPI_palloc(length);
        snprintf(result, length, "%s.%s = %s.%s", table, col, fTable, fCol);
    }
    SPI_finish();

    return result;
}

bool getRetroQuerySize(const void* item, void* s) {
    const struct WordVec *wv = item;
    int* sum = s;
    *sum += wv->dim * (2 + FLT_MANT_DIG) + strlen(wv->word) + 50;
    return true;
}

bool buildRetroQuery(const void* item, void* q) {
    const struct WordVec *wv = item;
    retroQuery *query = q;
    char* term = escape(wv->word);

    query->cur += sprintf(query->cur, "('%s', vec_to_bytea('{", term);
    for (int i = 0; i < wv->dim; i++) {
        if (i < wv->dim - 1) {
            query->cur += sprintf(query->cur, "%f, ", wv->vector[i]);
        } else {
            query->cur += sprintf(query->cur, "%f", wv->vector[i]);
        }
    }
    query->cur += sprintf(query->cur, "}'::float4[])), ");
    return true;
}

char* encodeEmbeddingForQuery(char* term, float* vector, int dim) {
  char* result = palloc0(strlen(term) + 15*dim + 100);
  char* cur = result;
  cur += sprintf(cur, "('%s', vec_to_bytea('{", escape(term));
  for (int i = 0; i < dim; i++) {
    if (i+1 < dim) {
      cur += sprintf(cur, "%f, ", vector[i]);
    } else {
      cur += sprintf(cur, "%f", vector[i]);
    }
  }
  sprintf(cur, "}'::float4[]))");
  return result;
}

void insertRetroVecsInDB(const char* tableName, struct hashmap* retroVecs, DeltaCat* deltaCat, int deltaCatCount, int dim) {
  char* query;
  char* queryCursor;
  WordVec* retroVec;
  int termCount = 0;
  for (int i = 0; i < deltaCatCount; i++) {
    DeltaCat* deltaCatEntry = deltaCat + i;
    termCount += deltaCatEntry->elementCount;
    termCount += deltaCatEntry->inferredElementCount;
  }
  query = palloc0((termCount+2)*200+(termCount*dim*15+1000));
  queryCursor = query;
  queryCursor += sprintf(query, "INSERT INTO %s (word, vector) VALUES ", tableName);
  for (int i = 0; i < deltaCatCount; i++) {
    DeltaCat* deltaCatEntry = deltaCat + i;
    for (int j = 0; j < deltaCatEntry->elementCount; j++) {
      char* word = deltaCatEntry->elements[j].name;
      char* term = palloc0(strlen(deltaCatEntry->name) + strlen(word) + 2);
      sprintf(term, "%s#%s", deltaCatEntry->name, word);
      retroVec = hashmap_get(retroVecs, &(WordVec){.word=term});
      if (retroVec != NULL) {
        char* vectorEntry = encodeEmbeddingForQuery(term, retroVec->vector, dim);
        if (((j + 1) < deltaCatEntry->elementCount) || (deltaCatEntry->inferredElementCount > 0) || ((i+1) < deltaCatCount)) {
          queryCursor += sprintf(queryCursor, "%s, ", vectorEntry);
        } else {
          queryCursor += sprintf(queryCursor, "%s ", vectorEntry);
        }
        pfree(vectorEntry);
      } else {
        elog(ERROR, "no retro vec trained for term %s in delta file", term);
      }
    }
    for (int j = 0; j < deltaCatEntry->inferredElementCount; j++) {
      char* word = deltaCatEntry->inferredElements[j].name;
      char* term = palloc0(strlen(deltaCatEntry->name) + strlen(word) + 2);
      sprintf(term, "%s#%s", deltaCatEntry->name, word);
      retroVec = hashmap_get(retroVecs, &(WordVec){.word=term});
      if (retroVec != NULL) {
        char* vectorEntry = encodeEmbeddingForQuery(term, retroVec->vector, dim);
        if (((j + 1) < deltaCatEntry->inferredElementCount) || ((i+1) < deltaCatCount)) {
          queryCursor += sprintf(queryCursor, "%s, ", vectorEntry);
        } else {
          queryCursor += sprintf(queryCursor, "%s ", vectorEntry);
        }
        pfree(vectorEntry);
      } else {
        elog(ERROR, "no retro vec trained for term %s in delta file", term);
      }
    }
  }
  sprintf(queryCursor, "ON CONFLICT (word) DO UPDATE SET vector = EXCLUDED.vector");
  SPI_connect();
  SPI_exec(query, 0);
  SPI_finish();
  pfree(query);
}

void deleteRetroVecsDB(const char* tableName) {
    char* command = palloc0(100 + strlen(tableName));
    sprintf(command, "TRUNCATE TABLE %s RESTART IDENTITY", tableName);
    if (SPI_connect() == SPI_OK_CONNECT) {
        SPI_exec(command, 0);
        SPI_finish();
    }
}

ColMean* loadColMeanFromDB(char* tabCol, int dim){
  char* command;
  ResultInfo rInfo;

  ColMean* result = palloc0(sizeof(ColMean));

  SPI_connect();
  command = palloc0(500);
  sprintf(command, "SELECT mean, size FROM column_stats WHERE name = '%s'", tabCol);
  rInfo.ret = SPI_exec(command, 0);
  rInfo.proc = SPI_processed;
  if (rInfo.ret > 0 && rInfo.proc > 0 && SPI_tuptable != NULL) {
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;
      Datum vector;
      Datum sizeValue;
      int n = 0;
      bytea* vectorData;
      float4* tmp;

      HeapTuple tuple = tuptable->vals[0];
      vector = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
      sizeValue = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);
      vectorData = DatumGetByteaP(vector);

      tmp = (float4 *) VARDATA(vectorData);
      result->vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(result->vector, tmp, n * sizeof(float4));
      result->size = DatumGetInt32(sizeValue);
      result->word = tabCol;
  }
  SPI_finish();
  return result;
}

struct hashmap* loadAllColMeansFromDB(ProcessedDeltaEntry* processedDelta, int processedDeltaCount, int dim) {
  struct hashmap* allColMeans = NULL;
  ProcessedDeltaEntry entry;
  hashmap_set_allocator(palloc0, pfree);
  allColMeans = hashmap_new(sizeof(ColMean), 0, 0, 0, colMeanHash, colMeanCompare, NULL);
  for (int i = 0; i < processedDeltaCount; i++) {
      entry = processedDelta[i];
      if (!hashmap_get(allColMeans, &(ColMean){ .word=entry.column })) {
          // add col_mean to hashmap
          ColMean* col_mean = loadColMeanFromDB(entry.column, dim);
          hashmap_set(allColMeans, col_mean);
      }
  }
  return allColMeans;
}

struct hashmap* loadAllRelColEntries(){
  struct hashmap* allRelColEntries = NULL;
  ResultInfo rInfo;
  hashmap_set_allocator(palloc0, pfree);
  allRelColEntries = hashmap_new(sizeof(RelColEntry), 0, 0, 0, RelColEntryHash, RelColEntryCompare, NULL);
  SPI_connect();
  rInfo.ret = SPI_exec("SELECT id, name, size, centroid FROM rel_col_stats", 0);
  rInfo.proc = SPI_processed;
  if (rInfo.ret > 0 && rInfo.proc > 0 && SPI_tuptable != NULL) {
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    for (int i = 0; i < rInfo.proc; i++) {
      HeapTuple tuple = tuptable->vals[i];
      Datum col_id;
      char* word;
      Datum size;
      Datum vector;
      int n = 0;
      bytea* vectorData;
      float4* tmp;

      RelColEntry* relColEntry = SPI_palloc(sizeof(RelColEntry));

      col_id = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
      word = (char*) SPI_getvalue(tuple, tupdesc, 2);
      size = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
      vector = SPI_getbinval(tuple, tupdesc, 4, &rInfo.info);
      if (rInfo.info != 0) {
        continue;
      }
      vectorData = DatumGetByteaP(vector);
      tmp = (float4 *) VARDATA(vectorData);

      relColEntry->id = DatumGetInt32(col_id);
      relColEntry->word = SPI_palloc(strlen(word) + 1);
      snprintf(relColEntry->word, strlen(word) + 1, "%s", word);
      relColEntry->size = DatumGetInt32(size);
      relColEntry->centroid = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
      n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
      memcpy(relColEntry->centroid, tmp, n * sizeof(float4));
      hashmap_set(allRelColEntries, relColEntry);
    }
  }
  SPI_finish();
  return allRelColEntries;
}

float* calcCentroid(ProcessedRel* relation, char* retroVecTable, int dim) {
    float* sum = palloc0(dim * sizeof(float));
    char* joinRel = NULL;
    char* selector;
    char* tableName1;
    char* tableName2;
    char* innerSelect = palloc0(300);

    char* command = palloc0(500);
    ResultInfo rInfo;
    float4* tmp;

    char** relKeys = getTableAndColFromRel(relation->key);

    tableName1 = relKeys[0];
    tableName2 = relKeys[2];

    if (strcmp(relation->target, "col1") == 0) {
        selector = palloc0(strlen(relKeys[0]) + strlen(relKeys[1]) + 2);
        sprintf(selector, "%s.%s", relKeys[0], relKeys[1]);

        if (strcmp(relKeys[4], "-") != 0) {
            tableName1 = relKeys[4];
            tableName2 = relKeys[0];
            joinRel = getJoinRelFromDB(relKeys[4], relKeys[0]);
        }
    } else {
        selector = palloc0(strlen(relKeys[2]) + strlen(relKeys[3]) + 2);
        sprintf(selector, "%s.%s", relKeys[2], relKeys[3]);

        if (strcmp(relKeys[4], "-") != 0) {
            tableName1 = relKeys[4];
            tableName2 = relKeys[2];
            joinRel = getJoinRelFromDB(relKeys[4], relKeys[2]);
        }
    }

    if (joinRel == NULL) {
        joinRel = getJoinRelFromDB(relKeys[0], relKeys[2]);
    }

    sprintf(innerSelect, "SELECT regexp_replace(%s, '[\\.#~\\s\\u00a0,\\(\\)/\\[\\]:]+', '_', 'g') AS term "
                         "FROM %s INNER JOIN %s ON %s", selector, tableName1, tableName2, joinRel);

    sprintf(command, "SELECT vector FROM ("
                     "SELECT DISTINCT * "
                     "FROM %s AS rtv JOIN (%s) as sub "
                     "ON rtv.word = CONCAT('%s#', sub.term) "
                     ") as res",
            retroVecTable, innerSelect, selector);

    SPI_connect();
    rInfo.ret = SPI_exec(command, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;

        for (int i = 0; i < rInfo.proc; i++) {
            Datum vector;
            bytea* vectorData;

            HeapTuple tuple = tuptable->vals[i];
            vector = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);

            if (rInfo.info == 0) {              // no NULL value at vector
                vectorData = DatumGetByteaP(vector);
                tmp = (float4 *) VARDATA(vectorData);
                sumVecs(sum, tmp, dim);
            }
        }
    }
    SPI_finish();

    return sum;
}

void updateCentroidInDB(ProcessedRel* relation, float* centroid, int dim) {
    char* command;
    char* cur;
    ResultInfo rInfo;

    if (SPI_connect() == SPI_OK_CONNECT) {
        command = palloc0(500 + dim * 15);
        cur = command;
        cur += sprintf(cur, "UPDATE rel_col_stats SET centroid_bin = vec_to_bytea('{");
        for (int i = 0; i < dim; i++) {
            if (i < dim - 1) {
                cur += sprintf(cur, "%f, ", centroid[i]);
            } else {
                cur += sprintf(cur, "%f", centroid[i]);
            }
        }
        sprintf(cur, "}'::float4[]) "
                     "WHERE id = (SELECT %s FROM relation_stats WHERE relation_name = '%s')",
                     relation->target, relation->key);

        rInfo.ret = SPI_execute(command, false, 0);
        if (rInfo.ret != SPI_OK_UPDATE) {
            elog(WARNING, "Failed to update relation centroid vector!");
        }
        rInfo.proc = SPI_processed;
        SPI_finish();
    }
}

float* getCentroidFromDB(ProcessedRel* relation) {
    char* command;
    ResultInfo rInfo;

    float* result = NULL;

    SPI_connect();

    command = palloc0(1000);
    sprintf(command, "SELECT centroid FROM relation_stats JOIN rel_col_stats ON relation_stats.%s = rel_col_stats.id WHERE relation_stats.relation_name = '%s'",
            relation->target, relation->key);

    rInfo.ret = SPI_exec(command, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret > 0 && rInfo.proc > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        Datum vector;
        int n = 0;
        bytea* vectorData;
        float4* tmp;

        HeapTuple tuple = tuptable->vals[0];
        vector = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);

        if (rInfo.info == 0) {
            vectorData = DatumGetByteaP(vector);
            tmp = (float4 *) VARDATA(vectorData);
            result = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
            n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
            memcpy(result, tmp, n * sizeof(float4));
        }
    }
    SPI_finish();
    return result;
}

float* getCentroid(ProcessedRel* relation, char* retroVecTable, int dim) {
    float* centroid = NULL;

    centroid = getCentroidFromDB(relation);

    if (centroid == NULL) {
        elog(ERROR, "Self calculation of centroid is deprecated!");
        centroid = calcCentroid(relation, retroVecTable, dim);
        updateCentroidInDB(relation, centroid, dim);
    }
    return centroid;
}

void* prealloc(void* ptr, size_t new_s) {
    if (ptr == NULL) {
        return palloc0(new_s);
    }
    return repalloc(ptr, new_s);
}

void processDeltaElements(ProcessedDeltaEntry* result, DeltaCat* deltaCat, int elementCount, DeltaElem* elements, int* index) {
    for (int i = 0; i < elementCount; i++) {
        result[*index].name = palloc0(strlen(deltaCat->name) + strlen(elements[i].name) + 2);
        sprintf(result[*index].name, "%s#%s", deltaCat->name, elements[i].name);

        result[*index].vector = palloc0(strlen(elements[i].value) + 1);
        sprintf(result[*index].vector, "%s", elements[i].value);

        result[*index].column = palloc0(strlen(deltaCat->name) + 1);
        sprintf(result[*index].column, "%s", deltaCat->name);

        result[*index].relationCount = 0;
        *index += 1;
    }
}

void addPrecessRel(ProcessedDeltaEntry* elem, DeltaRelElem* deltaRelElem, char* relation, char* term, char* target) {
    int added = 0;
    char* relKey = palloc0(strlen(relation) + strlen(deltaRelElem->name) + 2);
    sprintf(relKey, "%s:%s", relation, deltaRelElem->name);
    // elog(INFO, "relKey %s", relKey);
    for (int l = 0; l < elem->relationCount; l++) {
        // elog(INFO, "key is %s", (elem->relations + l)->key);
        if (strcmp(relKey, (elem->relations + l)->key) == 0) {
            (elem->relations + l)->terms = prealloc((elem->relations + l)->terms, sizeof(char*) * ((elem->relations + l)->termCount + 1));
            (elem->relations + l)->terms[(elem->relations + l)->termCount] = palloc0(strlen(term) + 1);
            memcpy((elem->relations + l)->terms[(elem->relations + l)->termCount], term, strlen(term));
            (elem->relations + l)->termCount += 1;
            added = 1;
            break;
        }
    }
    // elog(INFO, "added %d", added);
    if (!added) {

        elem->relations = prealloc(elem->relations, sizeof(ProcessedRel) * (elem->relationCount + 1));

        (elem->relations + elem->relationCount)->key = palloc0(strlen(relation) + strlen(deltaRelElem->name) + 2);
        sprintf((elem->relations + elem->relationCount)->key, "%s:%s", relation, deltaRelElem->name);

        (elem->relations + elem->relationCount)->terms = palloc(sizeof(char*));
        (elem->relations + elem->relationCount)->terms[0] = palloc0(strlen(term) + 1);
        memcpy((elem->relations + elem->relationCount)->terms[0], term, strlen(term));
        (elem->relations + elem->relationCount)->termCount = 1;

        (elem->relations + elem->relationCount)->target = palloc0(5);
        memcpy((elem->relations + elem->relationCount)->target, target, strlen(target));

        elem->relationCount += 1;

    }
}

ProcessedDeltaEntry* processDelta(DeltaCat* deltaCat, int deltaCatCount, DeltaRel* deltaRel, int deltaRelCount, int* count) {
    // int currentCount = 0;
    // int newCount = 0;
    ProcessedDeltaEntry* result = NULL;
    int index = 0;
    int num_entries = 0;

    const char* delim = "~";
    char* key1;
    char* key2;

    char** cols;
    char** toks;

    for (int i = 0; i < deltaCatCount; i++) {
      num_entries += (deltaCat + i)->elementCount + (deltaCat + i)->inferredElementCount;
    }

    result = palloc0(sizeof(ProcessedDeltaEntry) * num_entries);
    for (int i = 0; i < deltaCatCount; i++) {
        // elog(INFO, "process elements %d", (deltaCat + i)->elementCount);
        processDeltaElements(result, (deltaCat + i), (deltaCat + i)->elementCount, (deltaCat + i)->elements, &index);
        // elog(INFO, "process inferred elements %d %d", (deltaCat + i)->inferredElementCount, index);
        processDeltaElements(result, (deltaCat + i), (deltaCat + i)->inferredElementCount, (deltaCat + i)->inferredElements, &index);
    }
    elog(INFO, "finish processing");

    *count = num_entries;

    for (int i = 0; i < deltaRelCount; i++) {
        cols = getNToks((deltaRel + i)->relation, delim, 2);
        // elog(INFO, "get toks 0 cols0 %s cols1 %s", cols[0], cols[1]);
        for (int j = 0; j < (deltaRel + i)->groupCount; ++j) {
            for (int k = 0; k < (deltaRel + i)->groups[j].elementCount; k++) {
                toks = getNToks((deltaRel + i)->groups[j].elements[k].name, delim, 2);
                // elog(INFO, "get toks 1 toks0 %s toks1 %s", toks[0], toks[1]);
                key1 = palloc0(strlen(cols[0]) + strlen(toks[0]) + 2);
                key2 = palloc0(strlen(cols[1]) + strlen(toks[1]) + 2);
                sprintf(key1, "%s#%s", cols[0], toks[0]);
                sprintf(key2, "%s#%s", cols[1], toks[1]);

                for (int l = 0; l < num_entries; l++) {
                    if (strcmp(result[l].name, key1) == 0) {
                        addPrecessRel(&result[l], &(deltaRel + i)->groups[j], (deltaRel + i)->relation, key2, "col2");
                        // elog(INFO, "added relation to left elem");
                    }
                    if (strcmp(result[l].name, key2) == 0) {
                        addPrecessRel(&result[l], &(deltaRel + i)->groups[j], (deltaRel + i)->relation, key1, "col1");
                        // elog(INFO, "added relation to right elem");
                    }
                }
            }
        }
    }

    return result;
}

void addMissingVecs(struct hashmap* retroVecs, ProcessedDeltaEntry* processedDelta, int processedDeltaCount, RadixTree* vecTree, const char* tokenizationStrategy, int dim) {
    void* el = (void*)1;
    for (int i = 0; i < processedDeltaCount; i++) {
        if (!hashmap_get(retroVecs, &(WordVec){.word=processedDelta[i].name})) {
            WordVec* wv = palloc(sizeof(WordVec));
            wv->id = -1;
            wv->word = processedDelta[i].name;
            wv->vector = palloc0(dim*sizeof(float));
            inferVec(wv->vector, processedDelta[i].name, vecTree, "_", tokenizationStrategy, dim);
            wv->dim = dim;
            el = hashmap_set(retroVecs, wv);
            if (!el && hashmap_oom(retroVecs)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DIVISION_BY_ZERO),         // TODO: set correct error code (everywhere)
                        errmsg("hash map out of memory")));
            }
        }
    }
}

int radixCompare(const void *a, const void *b, void *udata) {
    const struct RadixTree *ta = a;
    const struct RadixTree *tb = b;
    return strcmp(ta->value, tb->value);
}

uint64_t radixHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct RadixTree *tree = item;
    return hashmap_murmur(tree->value, strlen(tree->value), seed0, seed1);
}

bool addEntryToRadixTree(const void* item, void* ref) {
    const WordVec* vec = item;
    RadixTree* tree = ref;
    char* delimiter = "_";
    char* t;
    char* ot;
    // char* split;
    void* el = (void*)1;

    RadixTree* current;
    RadixTree* foundTree;
    RadixTree* new;

    char* subword = NULL;
    char* old_subword = "";
    int subword_len = 1;

    bool add_ = false;
    bool _add = false;

    char* s = NULL;

    current = tree;

    t = palloc0(strlen(vec->word) + 1);
    ot = t;
    snprintf(t, strlen(vec->word) + 1, "%s", vec->word);

    if (t[strlen(t)-1] == '_'){
        add_ = true;
    }
    if (t[0] == '_'){
        _add = true;
    }

    if (_add){
        s = palloc0(sizeof(char));
    } else {
        s = strtok_r(t, delimiter, &t);
    }
    while (s != NULL) {
        char* s_new = strtok_r(t, delimiter, &t);
        if ((s_new == NULL) && add_){
          s_new = palloc0(sizeof(char*));
          add_ = false;
        }
        subword_len += strlen(s) + 1;
        subword = palloc0(subword_len*sizeof(char));
        sprintf(subword, "%s", old_subword);
        if (subword_len > (strlen(s) + 2)){
            sprintf(subword+strlen(old_subword), "_%s", s);
        }else{
            sprintf(subword+strlen(old_subword), "%s", s);
        }
        old_subword = subword;
        foundTree = NULL;
        if (current->children != NULL) {
            foundTree = hashmap_get(current->children, &(RadixTree){.value=subword});
        }

        if (foundTree != NULL) {
            current = foundTree;
        } else {
            if (current->children == NULL) {
                current->children = hashmap_new(sizeof(RadixTree), 0, 0, 0, radixHash, radixCompare, NULL);
            }

            new = palloc0(sizeof(RadixTree));
            new->id = vec->id;
            new->value = subword;
            new->vector = NULL;
            new->children = NULL;

            el = hashmap_set(current->children, new);
            if (!el && hashmap_oom(current->children)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DIVISION_BY_ZERO),
                        errmsg("hash map out of memory")));
            }

            new = hashmap_get(current->children, &(RadixTree){.value=subword});
            if (!new) {
                ereport(ERROR,
                    (errcode(ERRCODE_DIVISION_BY_ZERO),
                        errmsg("radix tree entry not found")));
            }
            current = new;
        }
        s = s_new;
    }
    current->vector = vec->vector;
    pfree(ot);
    return true;
}

RadixTree* buildRadixTree(struct hashmap* vecs, int dim) {
    RadixTree* result = palloc(sizeof(RadixTree));
    result->id = 0;
    result->value = NULL;
    result->vector = NULL;
    result->children = NULL;

    hashmap_scan(vecs, addEntryToRadixTree, result);

    return result;
}

void inferAdd(float* result, RadixTree* tree, int* tokens, const char* tokenizationStrategy, int dim) {
    if (strcmp(tokenizationStrategy, "log10") == 0) {
        float log;
        float* tmpVec = palloc0(dim * sizeof(float));
        log = (float)log10((double)tree->id);
        memcpy(tmpVec, tree->vector, dim * sizeof(float));
        multVecF(tmpVec, log, dim);
        sumVecs(result, tmpVec, dim);
        *tokens += log;
        pfree(tmpVec);
    } else {
        sumVecs(result, tree->vector, dim);
        *tokens += 1;
    }
}

void inferVec(float* result, const char* term, RadixTree* tree, char* delimiter, const char* tokenizationStrategy, int dim) {
    char* tmp;
    int index = 0;
    char *t;

    RadixTree* foundTree = NULL;
    RadixTree* lastMatch = NULL;
    RadixTree* current = tree;

    int tokens = 0;
    int i, j, lastJ;

    char** splits;
    char* split;
    int split_count = 1;
    bool final_ = false;
    bool _add;

    char* subword;
    int subword_len;
    int pos;

    tmp = strchr(term, '#');
    if (tmp != NULL) {
        index = (int)(tmp - term);
        index = index ? index + 1 : 0;
    }

    t = palloc0(strlen(term) + index + 1);
    strcpy(t, term + index);

    _add = (t[0] == '_');
    for (int i=0; t[i]; i++) {
        if(t[i]=='_'){
            if (!t[i+1]) {
              final_ = true;
              split_count++;
            } else {
              if (t[i+1] != '_') {
                split_count++;
              }
            }
        }
    }
    splits = palloc0(sizeof(char*)*(split_count+1));
    split = strtok_r(t, delimiter, &t);
    i = 0;
    if (_add){
      splits[i] = "";
      i++;
    }
    while (split != NULL){
        splits[i] = split;
        i++;
        split = strtok_r(t, delimiter, &t);
    }
    if (final_){
      splits[i] = "";
      i++;
    }


    i = 1;
    j = 0;
    lastJ = 0;
    while ((splits[i-1] != NULL) || (lastMatch != NULL)){
      if (i > split_count){
          // lastMatch should be true!
          inferAdd(result, lastMatch, &tokens, "simple", dim);
          j = lastJ;
          i = j+1;
          lastMatch = NULL;
          continue;
      }
      // create subword
      subword_len = 0;
      for (int k=j; k < i; k++){
          subword_len += strlen(splits[k]);
      }

      subword = palloc0((subword_len+(i-j)+1)*sizeof(char));
      pos = 0;
      for (int k=j; k < i; k++){
          sprintf(subword+pos, "%s", splits[k]);
          pos += strlen(splits[k]);
          if ((k+1) < i){
              sprintf(subword+pos, "_");
              pos++;
          }
      }
      // check if subword at current tree position
      foundTree = NULL;
      if (current->children) {
          foundTree = hashmap_get(current->children, &(RadixTree){.value=subword});
      }
      if (foundTree) {
          current = foundTree;
          if (current->vector) {
              // backup possible token
              lastMatch = current;
              lastJ = i;
          }
      }else{
          if (lastMatch){
              inferAdd(result, lastMatch, &tokens, "simple", dim);
              j = lastJ;
              i = j;
              lastMatch = NULL;
          }else{
              j++;
              i = j;
          }
          current = tree;
      }
      i += 1;
    }
    divVec(result, tokens, dim);
}

void sumVecs(float* vec1, float* vec2, int dim) {
    for (int i = 0; i < dim; i++) {
        vec1[i] += vec2[i];
    }
}

void add2Vec(float* vec1, float add, int dim) {
    for (int i = 0; i < dim; i++) {
        vec1[i] += add;
    }
}

void subFromVec(float* vec1, float sub, int dim) {
    for (int i = 0; i < dim; i++) {
        vec1[i] -= sub;
    }
}

void subVecs(float* vec1, float* vec2, int dim) {
    for (int i = 0; i < dim; i++) {
        vec1[i] -= vec2[i];
    }
}

void multVecs(float* vec1, float* vec2, int dim) {
    for (int i = 0; i < dim; i++) {
        vec1[i] *= vec2[i];
    }
}

void multVec(float* vec, int mult, int dim) {
    for (int i = 0; i < dim; i++) {
        vec[i] *= mult;
    }
}

void multVecF(float* vec, float mult, int dim) {
    for (int i = 0; i < dim; i++) {
        vec[i] *= mult;
    }
}

void divVec(float* vec, int divisor, int dim) {
    if (divisor == 0) return;
    for (int i = 0; i < dim; i++) {
        vec[i] /= divisor;
    }
}

void divVecF(float* vec, float divisor, int dim) {
    if (divisor == 0) return;
    for (int i = 0; i < dim; i++) {
        vec[i] /= divisor;
    }
}

void clearVec(float* vec, int dim) {
    for (int i = 0; i < dim; i++) {
        vec[i] = 0.0;
    }
}

int isZeroVec(float* vec, int dim) {
    for (int i = 0; i < dim; i++) {
        if (vec[i] != 0) {
            return 0;
        }
    }
    return 1;
}

void printVec(float* vec, int dim) {
    for (int i = 0; i < dim; i++) {
        elog(INFO, "%d: %f", i, vec[i]);
    }
}

char** getNToks(char* word, const char* delim, int size) {
    char* s = palloc0(strlen(word) + 1);
    char** result = palloc0(size * sizeof(char*));
    int i = 0;

    strncpy(s, word, strlen(word));

    for (char* p = strtok(s, delim); p != NULL; p = strtok(NULL, delim)) {
        result[i] = palloc0(strlen(p) + 1);
        strncpy(result[i], p, strlen(p));
        i++;
    }
    pfree(s);
    return result;
}

char** getTableAndCol(char* word) {
    char* s = palloc0(strlen(word) + 1);
    char** result = palloc0(2 * sizeof(char*));
    strcpy(s, word);
    result = getNToks(s, ".", 2);
    return result;
}

char** getTableAndColFromRel(char* word) {
    char** result = palloc0(5 * sizeof(char*));
    char** tmp = palloc0(2 * sizeof(char*));
    char** tabs = getNToks(word, "~", 2);
    tmp = getNToks(tabs[0], ".", 2);
    result[0] = tmp[0];
    result[1] = tmp[1];
    tmp = getNToks(tabs[1], ".", 2);
    result[2] = tmp[0];
    tmp = getNToks(tmp[1], ":", 2);
    result[3] = tmp[0];
    result[4] = tmp[1];

    return result;
}

void resizeQuery(char* query, int* currentSize, int maxNeeded) {
    if (*currentSize < maxNeeded) {
        elog(INFO, "resize query to %d", maxNeeded);
        pfree(query);
        query = palloc0(maxNeeded);
    }
}

int getInt(struct hashmap* map, char* query) {
    int res;
    dbElem* found = NULL;
    dbElem* el = NULL;

    if ((found = hashmap_get(map, &(dbElem){.key=query}))) {
        return found->value;
    }

    res = getIntFromDB(query);

    el = palloc(sizeof(dbElem));
    el->key = palloc0(strlen(query) + 1);
    sprintf(el->key, "%s", query);
    el->value = res;

    el = hashmap_set(map, el);
    if (!el && hashmap_oom(map)) {
        ereport(ERROR,
            (errcode(ERRCODE_DIVISION_BY_ZERO),
                errmsg("hash map out of memory")));
    }
    return res;
}

int dbElemCompare(const void *a, const void *b, void *data) {
    const struct dbElem *ela = a;
    const struct dbElem *elb = b;
    return strcmp(ela->key, elb->key);
}

uint64_t dbElemHash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct dbElem *elem = item;
    return hashmap_murmur(elem->key, strlen(elem->key), seed0, seed1);
}

struct hashmap* getRelationStats() {
  struct hashmap* relationStats = hashmap_new(sizeof(RelationStats), 0, 0, 0, RelationStatsHash, RelationStatsCompare, NULL);
  ResultInfo rInfo;

  SPI_connect();
  rInfo.ret = SPI_exec("SELECT relation_name, col1, col2, max_r FROM relation_stats", 0);
  rInfo.proc = SPI_processed;
  if (rInfo.ret > 0 && rInfo.proc > 0 && SPI_tuptable != NULL) {
      TupleDesc tupdesc = SPI_tuptable->tupdesc;
      SPITupleTable *tuptable = SPI_tuptable;
      for (int i = 0; i < rInfo.proc; i++) {
          Datum id_col1;
          Datum id_col2;
          Datum max_r;
          char* word;
          RelationStats* relationStatsObj = SPI_palloc(sizeof(RelationStats));

          HeapTuple tuple = tuptable->vals[i];
          word = (char*) SPI_getvalue(tuple, tupdesc, 1);
          id_col1 = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);
          id_col2 = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
          max_r = SPI_getbinval(tuple, tupdesc, 4, &rInfo.info);
          relationStatsObj->col1 = DatumGetInt32(id_col1);
          relationStatsObj->col2 = DatumGetInt32(id_col2);
          relationStatsObj->max_r = DatumGetInt32(max_r);
          relationStatsObj->name = SPI_palloc(strlen(word) + 1);
          snprintf(relationStatsObj->name, strlen(word) + 1, "%s", word);
          // elog(INFO, "set relation with name %s col1 %d col2 %d", relationStatsObj->name, relationStatsObj->col1, relationStatsObj->col2);
          hashmap_set(relationStats, relationStatsObj);
      }
  }
  SPI_finish();

  return relationStats;
}

bool iterUpdateCardinalities(const void* item, void* data){
  const CardinalityCount* c = item;
  char** updateQueryPointer = data;
  (*updateQueryPointer) += sprintf(*updateQueryPointer, "('%s', %d, %d), ", escape(c->word), c->count, c->colId);
  return true;
}

bool iterRelNumStats(const void* item, void* data){
  const RelNumEntry* r = item;
  char** queryPointer = data;
  (*queryPointer) += sprintf(*queryPointer, "('%s', %d), ", escape(r->word), r->rel_num);
  return true;
}

void storeColMean(char* catName, float* new_mean, int size, int dim) {
  char* updateColMeanQuery = palloc0(sizeof(char)*dim*10+100);
  char* cur = updateColMeanQuery;
  cur += sprintf(cur, "UPDATE column_stats SET size = %d, mean = vec_to_bytea('{", size);
  for (int i = 0; i < dim; i++) {
      if (i < dim - 1) {
          cur += sprintf(cur, "%f, ", new_mean[i]);
      } else {
          cur += sprintf(cur, "%f", new_mean[i]);
      }
  }
  sprintf(cur, "}'::float4[]) WHERE name = '%s'", catName);
  SPI_connect();
  SPI_exec(updateColMeanQuery, 0);
  SPI_finish();
  pfree(updateColMeanQuery);
}

void storeRelColEntry(int colId, float* newCentroid, int size, int dim) {
  char* updateQuery = palloc0(sizeof(char)*dim*10+200);
  char* cur = updateQuery + sprintf(updateQuery, "UPDATE rel_col_stats SET size = %d, centroid = vec_to_bytea('{", size);
  for (int i = 0; i < dim; i++){
    if (i < dim - 1){
      cur += sprintf(cur, "%f, ", newCentroid[i]);
    } else {
      cur += sprintf(cur, "%f", newCentroid[i]);
    }
  }
  sprintf(cur, "}'::float4[]) WHERE id = %d", colId);
  SPI_connect();
  SPI_exec(updateQuery, 0);
  SPI_finish();
  pfree(updateQuery);
}

struct hashmap* determineChangedElements(DeltaCat* deltaCat, int deltaCatCount) {
  struct hashmap* deltaWords = hashmap_new(sizeof(DeltaWord), 0, 0, 0, DeltaWordHash, DeltaWordCompare, NULL);
  DeltaCat* catEntry;
  for (int i = 0; i < deltaCatCount; i++){
    catEntry = (deltaCat + i);
    for (int j = 0; j < catEntry->elementCount; j++){
      DeltaWord* newDeltaWord = palloc0(sizeof(DeltaWord));
      newDeltaWord->word = catEntry->elements[j].name;
      newDeltaWord->changed = false;
      hashmap_set(deltaWords, newDeltaWord);
    }
    for (int j = 0; j < catEntry->inferredElementCount; j++){
      DeltaWord* newDeltaWord = palloc0(sizeof(DeltaWord));
      newDeltaWord->word = catEntry->inferredElements[j].name;
      newDeltaWord->changed = false;
      hashmap_set(deltaWords, newDeltaWord);
    }
    for (int j = 0; j < catEntry->changedElementsCount; j++){
      char* term = catEntry->changedElements[j];
      DeltaWord* deltaWord = hashmap_get(deltaWords, &(DeltaWord){.word =term});
      deltaWord->changed = true;
    }
  }
  return deltaWords;
}

void updateStatsBefore(struct hashmap* deltaWords, DeltaCat* deltaCat, DeltaRel* deltaRel, int deltaCatCount, int deltaRelCount) {
  DeltaCat* catEntry;
  DeltaRel* relEntry;
  DeltaWord* ckeckDelta;
  struct hashmap* cardinalities = hashmap_new(sizeof(CardinalityCount), 0, 0, 0, CardinalityCountHash, CardinalityCountCompare, NULL);
  struct hashmap* rel_num_stats = hashmap_new(sizeof(RelNumEntry), 0, 0, 0, RelNumEntryHash, RelNumEntryCompare, NULL);
  int cardinalityCount = 0;
  int relNumStatsCount = 0;
  int maxRelTypes = 0;
  struct hashmap* relationStats = getRelationStats();
  char** colNames;
  RelationStats* relStats;
  int col1, col2;
  CardinalityCount* cardinalityElem;
  char* searchTerm;
  RelNumEntry* relNumEntry;
  char *cardinalityUpdateQuery, *cardinalityQueryPointer;
  char *rel_num_stats_query, *rel_num_stats_query_pointer;
  for (int i = 0; i < deltaCatCount; i++){
    catEntry = (deltaCat + i);
    cardinalityCount += catEntry->elementCount + catEntry->inferredElementCount;
  }
  for (int i = 0; i < deltaRelCount; i++){
    relEntry = (deltaRel + i);
    maxRelTypes += relEntry->groupCount * 2;
  }
  for (int i = 0; i < deltaRelCount; i++){
    relEntry = (deltaRel + i);
    colNames = getNToks(relEntry->relation, "~", 2);
    for (int k = 0; k < relEntry->groupCount; k++){
      DeltaRelElem* relGroup = (relEntry->groups + k);
      char* fullname = palloc0(strlen(relEntry->relation) + strlen(relGroup->name) + 2);
      sprintf(fullname, "%s:%s", relEntry->relation, relGroup->name);
      relStats = hashmap_get(relationStats, &(RelationStats){.name=fullname});
      col1 = relStats->col1;
      col2 = relStats->col2;
      for (int j = 0; j < relGroup->elementCount; j++){
        char* relatedTerms = relGroup->elements[j].name;
        char** terms = getNToks(relatedTerms, "~", 2);
        // update cardinalities
        for (int x = 0; x < 2; x++){
          int relColId = 0;
          int nonRelColId = 0;
          if (x == 0) {
            relColId = col1;
            nonRelColId = col2;
          } else {
            relColId = col2;
            nonRelColId = col1;
          }
          // check if terms is in delta file - otherwise cardinality and rel_num aggregates are not updated
          ckeckDelta = hashmap_get(deltaWords, &(DeltaWord){.word=terms[x]});
          if (ckeckDelta == NULL) {
            continue;
          }
          // increment cardinalities
          cardinalityElem = hashmap_get(cardinalities, &(CardinalityCount){.word=terms[x], .colId=relColId});
          if (cardinalityElem != NULL) {
            cardinalityElem->count++;
          } else {
            char* t = palloc0(strlen(terms[x])+1);
            cardinalityElem = palloc0(sizeof(CardinalityCount));
            sprintf(t, "%s", terms[x]);
            cardinalityElem->word = t;
            cardinalityElem->count = 1;
            cardinalityElem->colId = relColId;
            hashmap_set(cardinalities, cardinalityElem);
          }
          // increment rel_num_stats
          searchTerm = palloc0(strlen(colNames[x])+strlen(terms[x])+2);
          sprintf(searchTerm, "%s#%s", colNames[x], terms[x]);
          relNumEntry = hashmap_get(rel_num_stats, &(RelNumEntry){.word=searchTerm});
          if (relNumEntry == NULL) {
            RelNumEntry* newRelNumEntry = palloc0(sizeof(RelNumEntry));
            newRelNumEntry->word = palloc0(strlen(searchTerm)+1);
            sprintf(newRelNumEntry->word, "%s", searchTerm);
            newRelNumEntry->relations = palloc0(sizeof(bool)*(maxRelTypes+1));
            newRelNumEntry->relations[nonRelColId] = true;
            newRelNumEntry->rel_num = 1;
            relNumStatsCount += 1;
            hashmap_set(rel_num_stats, newRelNumEntry);
          } else {
            if (relNumEntry->relations[nonRelColId] == false){
              relNumEntry->relations[nonRelColId] = true;
              relNumEntry->rel_num += 1;
            }
          }
          pfree(searchTerm);
        }
      }
    }
  }
  // update cardinalities in DB
  cardinalityUpdateQuery = palloc0(sizeof(char)*(cardinalityCount+1)*200+1000);
  // char* cardinalityQueryPointer = cardinalityUpdateQuery + sprintf(cardinalityUpdateQuery, "INSERT INTO new_cardinalities (value, cardinality, col_id) VALUES ");
  cardinalityQueryPointer = cardinalityUpdateQuery + sprintf(cardinalityUpdateQuery, "INSERT INTO cardinality_stats (value, cardinality, col_id) VALUES ");
  hashmap_scan(cardinalities, iterUpdateCardinalities, &cardinalityQueryPointer);
  cardinalityQueryPointer[-2] = ' ';
  sprintf(cardinalityQueryPointer, "ON CONFLICT (value, col_id) DO UPDATE SET cardinality = EXCLUDED.cardinality");
  SPI_connect();
  SPI_exec(cardinalityUpdateQuery, 0);
  SPI_finish();
  pfree(cardinalityUpdateQuery);

  // update rel_num_stats in DB
  rel_num_stats_query = palloc0(sizeof(char)*(relNumStatsCount+1)*200+1000);
  rel_num_stats_query_pointer = rel_num_stats_query + sprintf(rel_num_stats_query, "INSERT INTO rel_num_stats (rel, value) VALUES ");
  hashmap_scan(rel_num_stats, iterRelNumStats, &rel_num_stats_query_pointer);
  rel_num_stats_query_pointer[-2] = ' ';
  sprintf(rel_num_stats_query_pointer, "ON CONFLICT (rel) DO UPDATE SET value = EXCLUDED.value");
  SPI_connect();
  SPI_exec(rel_num_stats_query, 0);
  SPI_finish();
  pfree(rel_num_stats_query);
}

void updateStatsAfter(struct hashmap* deltaWords, DeltaCat* deltaCat, DeltaRel* deltaRel, int deltaCatCount, int deltaRelCount, struct hashmap* retroVecs, struct hashmap* oldVecs, struct hashmap* allColMeans, int dim) {
  DeltaCat* catEntry;
  DeltaRel* relEntry;

  struct hashmap* relationStats;
  struct hashmap* rel_col_stats;

  float* new_mean;

  char** terms;
  int col1, col2;
  char** colNames;

  char** newWords;
  float* newVector;
  ColMean* colMean;

  DeltaWord* deltaWord;
  RelationStats* relStats;

  DeltaWord* checkDelta;
  DeltaWord* isAdded;

  struct hashmap* deltaRelWords;

  relationStats = getRelationStats();
  elog(INFO, "retrieve rel_col_stats ...");
  rel_col_stats = loadAllRelColEntries();
  elog(INFO, "update stats ...");

  // update column stats
  for (int i = 0; i < deltaCatCount; i++) {
    int newWordsSize = 0;
    catEntry = (deltaCat + i);
    newWords = palloc0(sizeof(char*)*(catEntry->elementCount+catEntry->inferredElementCount));
    newVector = palloc0(sizeof(float)*dim);
    // get column_mean
    colMean = hashmap_get(allColMeans, &(ColMean){.word = catEntry->name});
    for (int j = 0; j < (catEntry->elementCount + catEntry->inferredElementCount); j++){
      int k = (j < catEntry->elementCount) ? j : j - catEntry->elementCount;
      char* word = (j < catEntry->elementCount) ? catEntry->elements[k].name : catEntry->inferredElements[k].name;
      char* fullname = palloc0(strlen(catEntry->name) + strlen(word) + 2);
      sprintf(fullname, "%s#%s", catEntry->name, word);

      deltaWord = hashmap_get(deltaWords, &(DeltaWord){.word = word});
      if (deltaWord != NULL) {
        if (deltaWord->changed) {
          WordVec* retroVec = hashmap_get(oldVecs, &(WordVec) {.word=fullname});
          newWordsSize += 1;
          newWords[newWordsSize] = word;
          sumVecs(newVector, retroVec->vector, dim);
        }
      }
      pfree(fullname);
    }

    new_mean = palloc0(sizeof(float)*dim);
    memcpy(new_mean, colMean->vector, dim*sizeof(float));

    // add new vectors
    multVecF(new_mean, ((float) colMean->size)/((float) colMean->size + newWordsSize), dim);
    multVecF(newVector, (1.0/((float) colMean->size + newWordsSize)), dim);
    sumVecs(new_mean, newVector, dim);

    elog(INFO, "old mean %f new mean %f", colMean->vector[0], new_mean[0]);

    // store new col mean
    elog(INFO, "newWordsSize %d column_name %s", newWordsSize, catEntry->name);
    storeColMean(catEntry->name, new_mean, colMean->size + newWordsSize, dim);

    pfree(newVector);
    pfree(new_mean);
  }

  // update rel_col_stats

  for (int i = 0; i < deltaRelCount; i++){
    relEntry = (deltaRel + i);
    for (int k = 0; k < relEntry->groupCount; k++){
      float* newVectors[2] = { palloc0(sizeof(float)*dim), palloc0(sizeof(float)*dim) };
      int vectorCounts[2] = {0, 0};
      struct hashmap* alreadyAdded = hashmap_new(sizeof(DeltaWord), 0, 0, 0, DeltaWordHash, DeltaWordCompare, NULL);
      DeltaRelElem* relGroup = (relEntry->groups + k);
      char* fullname = palloc0(strlen(relEntry->relation) + strlen(relGroup->name) + 2);
      sprintf(fullname, "%s:%s", relEntry->relation, relGroup->name);
      relStats = hashmap_get(relationStats, &(RelationStats){.name=fullname});
      col1 = relStats->col1;
      col2 = relStats->col2;
      colNames = getNToks(relEntry->relation, "~", 2);

      // determine changed elements
      deltaRelWords = hashmap_new(sizeof(DeltaWord), 0, 0, 0, DeltaWordHash, DeltaWordCompare, NULL);
      for (int j = 0; j < relGroup->elementCount; j++){
        DeltaWord* newDeltaRelWord = palloc0(sizeof(DeltaWord));
        newDeltaRelWord->word = relGroup->elements[j].name;
        newDeltaRelWord->changed = false;
        hashmap_set(deltaRelWords, newDeltaRelWord);
      }
      for (int j = 0; j < relGroup->changedElementsCount; j++){
        char* term = relGroup->changedElements[j];
        DeltaWord* deltaRelWord = hashmap_get(deltaRelWords, &(DeltaWord){.word =term});
        deltaRelWord->changed = true;
      }

      // determine newVectors and changedVectors
      for (int j = 0; j < relGroup->elementCount; j++){
        char* relatedTerms = relGroup->elements[j].name;
        DeltaWord* deltaRelWord = hashmap_get(deltaRelWords, &(DeltaWord){.word =relatedTerms});
        if (!deltaRelWord->changed) {
          continue;
        }
        // split relatedTerms
        terms = getNToks(relatedTerms, "~", 2);
        if (deltaRelWord != NULL){
          for (int x = 0; x < 2; x++){
            char* fullword = palloc0(strlen(colNames[x]) + strlen(terms[x]) + 2);
            sprintf(fullword, "%s#%s", colNames[x], terms[x]);
            checkDelta = hashmap_get(deltaWords, &(DeltaWord){.word = terms[x]});
            isAdded = hashmap_get(alreadyAdded, &(DeltaWord){.word = terms[x]});
            if (checkDelta != NULL) {
              if(isAdded == NULL){
                if (checkDelta->changed) {
                  WordVec* newRetroVec = hashmap_get(retroVecs, &(WordVec) {.word=fullword});
                  if (newRetroVec == NULL) {
                    newRetroVec = hashmap_get(oldVecs, &(WordVec) {.word=fullword});
                  }
                  if (newRetroVec == NULL) {
                    elog(INFO, "Term %s is missing", fullword);
                    continue;
                  }
                  sumVecs(newVectors[x], newRetroVec->vector, dim);
                  vectorCounts[x] += 1;
                  hashmap_set(alreadyAdded, checkDelta);
                }
              }
            } else {
              continue;
            }
          }
        } else {
          elog(ERROR, "could not find relation %s in deltaRelWords", relatedTerms);
        }
      }

      // update rel_col_stats
      for (int x = 0; x < 2; x++){
        int colId = ( x==0 ) ? col1 : col2;
        // recalculate centroid in rel_num_stats
        float* newCentroid = palloc0(sizeof(float)*dim);
        RelColEntry* relColEntry = hashmap_get(rel_col_stats, &(RelColEntry) {.id=colId});
        memcpy(newCentroid, relColEntry->centroid, sizeof(float)*dim);
        sumVecs(newCentroid, newVectors[x], dim);
        storeRelColEntry(colId, newCentroid, relColEntry->size + vectorCounts[x], dim);
      }

      pfree(fullname);
    }
  }
}

struct hashmap* calcRetroVecs(ProcessedDeltaEntry* processedDelta, int processedDeltaCount, struct hashmap* retroVecs, RetroConfig* retroConfig, RadixTree* vecTree, retroPointer* pointer, struct hashmap* allColMeans, int dim) {
    const bool DEBUG_VECTORS = false;

    ProcessedDeltaEntry entry;
    ProcessedRel relation;

    char* query;

    float localBeta;
    float* nominator = palloc0(dim * sizeof(float4));
    float denominator;

    float* centroid;
    float gamma;
    float gamma_inv;
    float* delta = palloc0(dim * sizeof(float4));
    int max_r;
    int max_c;
    int size;
    int cardinality;
    float* retroVec = palloc0(dim * sizeof(float4));
    float* tmpVec = palloc0(dim * sizeof(float4));
    float* tmpOldVec;
    char* term;
    int value;
    void* el = (void*)1;

    char** entry_term_splits;
    char** tmp;

    float* currentVec = palloc0(dim * sizeof(float4));

    ColMean* col_mean;
    RetroExport* exportVec;
    WordVec* rwv;

    WordVec* wv;
    struct hashmap* result = hashmap_new(sizeof(WordVec), processedDeltaCount, 0, 0, wordVecHash, wordVecCompare, NULL);

    char* originalTableName = palloc0(100);
    char* retroVecTable = palloc0(100);

    // variables for intermediate results
    float* col_mean_local_beta = palloc0(dim*sizeof(float4));

    // just for debugging
    struct hashmap* exportVecs = hashmap_new(sizeof(RetroExport), 0, 0, 0, retroExportHash, retroExportCompare, NULL);

    getTableName(ORIGINAL, originalTableName, 100);
    getTableName(RETRO_VECS, retroVecTable, 100);

    // variable for all col means: word -> vector
    // allColMeans = loadAllColMeansFromDB(processedDelta, processedDeltaCount, dim);
    for (int i = 0; i < processedDeltaCount; i++) {
        currentVec = palloc0(dim*sizeof(float));
        pfree(currentVec);
        entry = processedDelta[i];
        entry_term_splits = getNToks(entry.name, "#", 2);
        col_mean = hashmap_get(allColMeans, &(ColMean){ .word=entry.column });
        if (col_mean == NULL){
          elog(ERROR, "no col mean for %s", entry.column);
        }
        // elog(INFO, "got col means %f", col_mean->vector[299]);
        localBeta = (float)retroConfig->beta / (entry.relationCount + 1);
        currentVec = palloc0(dim*sizeof(float));
        inferVec(currentVec, entry_term_splits[1], vecTree, "_", retroConfig->tokenization, dim);

        if (DEBUG_VECTORS){
          exportVec = palloc0(sizeof(RetroExport));
          exportVec->word = entry.name;
          tmpOldVec = palloc(dim*sizeof(float4));
          memcpy(tmpOldVec, currentVec, dim * sizeof(float4));
          exportVec->oldVector = tmpOldVec;
        }
        // elog(INFO, "currentVec %f %f", currentVec[0], currentVec[299]);
        memcpy(nominator, currentVec, dim * sizeof(float4));
        pfree(currentVec);
        multVec(nominator, retroConfig->alpha, dim);
        // sumVecs(nominator, col_mean, dim); // TODO check this
        //
        // // pfree(col_mean);
        // add2Vec(nominator, localBeta, dim); // TODO check this

        // -- new --
        memcpy(col_mean_local_beta, col_mean->vector, dim * sizeof(float4));
        multVecF(col_mean_local_beta, localBeta, dim);
        sumVecs(nominator, col_mean_local_beta, dim);
        // ---------
        denominator = retroConfig->alpha + localBeta;
        for (int j = 0; j < entry.relationCount; j++) {
            // elog(INFO, "process relation %d of %d", j, entry.relationCount);
            relation = entry.relations[j];
            // elog(INFO, "relation key %s", relation.key);
            // elog(INFO, "term before %s", relation.terms[14]);
            gamma = ((float)retroConfig->gamma) / (relation.termCount * (entry.relationCount + 1));
            // elog(INFO, "gamma %f", gamma);
            query = palloc0(2000);
            sprintf(query, "SELECT max_r FROM relation_stats WHERE relation_name = '%s'", relation.key);
            max_r = getInt(pointer->max_r_map, query);
            pfree(query);
            // sprintf(query, "SELECT max_c FROM relation_stats WHERE relation_name = '%s'", relation.key);
            // max_c = getInt(pointer->max_c_map, query);
            query = palloc0(2000);
            sprintf(query, "SELECT GREATEST(c1.size,c2.size) as g FROM relation_stats INNER JOIN rel_col_stats AS c1 ON col1 = c1.id INNER JOIN rel_col_stats AS c2 ON col2 = c2.id WHERE relation_name = '%s'", relation.key);
            max_c = getInt(pointer->max_c_map, query);
            pfree(query);
            // elog(INFO, "max_r %d max_c %d", max_r, max_c);
            centroid = getCentroid(&relation, retroVecTable, dim);
            // elog(INFO, "centroid %f %f", centroid[0], centroid[299]);
            memcpy(delta, centroid, dim * sizeof(float4));
            pfree(centroid);
            // elog(INFO, "delta-0 %f %f", delta[0], delta[299]);
            multVecF(delta, (float)retroConfig->delta * 2 / (max_c * (max_r + 1)), dim);
            // elog(INFO, "delta-1 %f %f", delta[0], delta[299]);
            subVecs(nominator, delta, dim);
            // resizeQuery(query, &querySize, strlen(relation.target) + strlen(relation.key) + 200);
            query = palloc0(strlen(relation.target) + strlen(relation.key) + 200);
            sprintf(query, "SELECT size FROM rel_col_stats WHERE id = (SELECT %s FROM relation_stats WHERE relation_name = '%s')",
                    relation.target, relation.key);
            size = getInt(pointer->size_map, query);
            pfree(query);
            // elog(INFO, "size %d denominator %f", size, denominator);
            denominator -= ((float)retroConfig->delta * 2 * size) / ((float) (max_c * (max_r + 1)));
            // elog(INFO, "nominator-2 %f %f denominator-2 %f", nominator[0], nominator[299], denominator);
            for (int k = 0; k < relation.termCount; k++) {
                // elog(INFO, "process term %d of %d", k, relation.termCount);
                term = relation.terms[k];
                // resizeQuery(query, &querySize, strlen(escape(term)) + 100);
                query = palloc0(strlen(term) + 200);
                sprintf(query, "SELECT value FROM rel_num_stats WHERE rel = '%s'", escape(term));
                value = getInt(pointer->value_map, query);
                pfree(query);
                // elog(INFO, "rel_num_inv %d", value);
                if (value == -1) {
                    elog(INFO, "term %s %s  %d", term, escape(term), k);
                    ereport(ERROR,
                        (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("value not found in DB")));
                }

                tmp = getNToks(term, "#", 2);
                query = palloc0(2000);
                sprintf(query, "SELECT cardinality FROM cardinality_stats "
                               "JOIN rel_col_stats ON cardinality_stats.col_id = rel_col_stats.id "
                               "WHERE rel_col_stats.name = '%s' AND value = '%s'",
                        escape(tmp[0]), escape(tmp[1]));
                cardinality = getInt(pointer->cardinality_map, query);
                pfree(query);
                if (cardinality == -1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("cardinality not found in DB")));
                }

                gamma_inv = ((float) retroConfig->gamma) / (float) (cardinality * (value + 1));
                // elog(INFO, "gamma_inv %f term %s", gamma_inv, tmp[1]);
                pfree(tmp[0]);
                pfree(tmp[1]);
                // pfree(tmp);
                rwv = hashmap_get(retroVecs, &(WordVec) {.word=term});
                if (rwv) {
                    memcpy(retroVec, rwv->vector, dim * sizeof(float4));
                }
                memcpy(tmpVec, retroVec, dim * sizeof(float4));
                multVecF(tmpVec, (gamma + gamma_inv), dim);
                sumVecs(nominator, tmpVec, dim);
                denominator += gamma + gamma_inv;

                memcpy(tmpVec, retroVec, dim * sizeof(float4));
                multVecF(tmpVec, (float)retroConfig->delta * 2 / ((max_r + 1) * max_c), dim);
                sumVecs(nominator, tmpVec, dim);
                denominator += (float)retroConfig->delta * 2 / ((max_r + 1) * max_c);
            }
        }
        // elog(INFO, "finally calculate vector");
        wv = palloc(sizeof(WordVec));
        wv->word = entry.name;
        wv->vector = palloc0(dim * sizeof(float4));
        memcpy(wv->vector, nominator, dim * sizeof(float4));
        multVecF(wv->vector, 1.0 / denominator, dim);
        if (DEBUG_VECTORS){
          float* tmp_nominator = palloc0(dim * sizeof(float4));
          memcpy(tmp_nominator, nominator, dim * sizeof(float4));
          exportVec->nominator = tmp_nominator;
          exportVec->denominator = denominator;
          exportVec->newVector =  wv->vector;
          hashmap_set(exportVecs, exportVec);
        }
        el = hashmap_set(result, wv);
        if (!el && hashmap_oom(result)) {
            ereport(ERROR,
               (errcode(ERRCODE_DIVISION_BY_ZERO),
                    errmsg("hash map out of memory")));
        }
        // elog(INFO, "finished setting vector %d %d", i, processedDeltaCount);
    }

    // elog(INFO, "finished calculation");

    if (DEBUG_VECTORS){
      FILE *fp;
      elog(INFO, "NEW VECTORS:");
      fp = fopen("/tmp/test.txt", "w+");
      elog(INFO, "created file pointer");
      hashmap_scan(exportVecs, iterOutput, fp);
      fclose(fp);
    }

    pfree(nominator);
    pfree(delta);
    pfree(retroVec);
    pfree(tmpVec);
    // pfree(query);
    pfree(originalTableName);
    pfree(retroVecTable);
    pfree(col_mean_local_beta);

    return result;
}

bool iterOutput(const void* item, void* data){
  const RetroExport* elem = item;
  FILE *fp = data;
  // fprintf(fp, "test\n");
  if (strcmp(elem->word, "reviews.review#EDIT_Video_stutters_time_now_Note_good_all_No_way_submit_report_app_PREVIOUS_REVIEW_improved_quite_bit_since_new_version_first_released_I_like_separate_alerts_I_turn_Programming_Alerts_example_A_dark_theme_showed_news_page_long_ago_I_really_liked_it_Hope_becomes_option_future_Overall_good_getting_better_") == 0){
    for (int i = 0; i < 300; i++){
      fprintf(fp,  "%f %f\n", elem->oldVector[i], elem->newVector[i]);
      // fprintf(fp,  "%s %f %f\n", elem->word, elem->oldVector[0], elem->newVector[0]);
      // elog(INFO, "%s %f %f", elem->word, elem->oldVector[0], elem->newVector[0]);
    }
  }
  return true;
}

bool iterStoreVectors(const void* item, void* data){
  const WordVec* elem = item;
  FILE *fp = data;
  // fprintf(fp, "test\n");
  fprintf(fp, "%s", elem->word);
  for (int i = 0; i < 300; i++){
    fprintf(fp,  " %f", elem->vector[i]);
    // fprintf(fp,  "%s %f %f\n", elem->word, elem->oldVector[0], elem->newVector[0]);
    // elog(INFO, "%s %f %f", elem->word, elem->oldVector[0], elem->newVector[0]);
  }
  fprintf(fp, "\n");

  return true;
}



bool iterUpdate(const void* item, void* data) {
    const WordVec* wv = item;
    WordVec* retroVec;
    l2IterStr* str = data;
    void* el = (void*)1;

    if ((retroVec = hashmap_get(str->vecs, &(WordVec){.word=wv->word}))) {
        pfree(retroVec->vector);
        retroVec->vector = wv->vector;
        el = hashmap_set(str->vecs, retroVec);
        if (!el && hashmap_oom(str->vecs)) {
            ereport(ERROR,
                (errcode(ERRCODE_DIVISION_BY_ZERO),
                    errmsg("hash map out of memory")));
        }
    } else {
        retroVec = palloc(sizeof(WordVec));
        retroVec->id = wv->id;
        retroVec->dim = wv->dim;
        retroVec->word = palloc0(strlen(wv->word) + 1);
        sprintf(retroVec->word, "%s", wv->word);
        retroVec->vector = palloc0(wv->dim * sizeof(float));
        memcpy(retroVec->vector, wv->vector, wv->dim * sizeof(float));
        el = hashmap_set(str->vecs, retroVec);
        if (!el && hashmap_oom(str->vecs)) {
            ereport(ERROR,
                (errcode(ERRCODE_DIVISION_BY_ZERO),
                    errmsg("hash map out of memory")));
        }
    }
    return true;
}

struct hashmap* backupOldRetroVecs(struct hashmap* retroVecs, DeltaCat* deltaCat, int deltaCatCount, int dim) {
  struct hashmap* result = hashmap_new(sizeof(WordVec), 0, 0, 0, wordVecHash, wordVecCompare, NULL);
  for (int i = 0; i < deltaCatCount; i++) {
    DeltaCat* deltaCatEntry = deltaCat + i;
    for (int j = 0; j < deltaCatEntry->elementCount; j++) {
      WordVec* retroVec;
      char* word = deltaCatEntry->elements[j].name;
      char* term = palloc0(strlen(deltaCatEntry->name) + strlen(word) + 2);
      sprintf(term, "%s#%s", deltaCatEntry->name, word);
      retroVec = hashmap_get(retroVecs, &(WordVec){.word=term});
      if (retroVec != NULL) {
        WordVec* newWordVec = palloc0(sizeof(WordVec));
        newWordVec->vector = palloc0(sizeof(float)*dim);
        memcpy(newWordVec->vector, retroVec->vector, dim*sizeof(float));
        newWordVec->word = term; // palloc0(strlen(retroVec->word)+1);
        // sprintf(newWordVec->word, "%s", retroVec->word);
        newWordVec->id = retroVec->id;
        newWordVec->dim = retroVec->dim;
        hashmap_set(result, newWordVec);
      }
    }
    for (int j = 0; j < deltaCatEntry->inferredElementCount; j++) {
      WordVec* retroVec;
      char* word = deltaCatEntry->inferredElements[j].name;
      char* term = palloc0(strlen(deltaCatEntry->name) + strlen(word) + 2);
      sprintf(term, "%s#%s", deltaCatEntry->name, word);
      retroVec = hashmap_get(retroVecs, &(WordVec){.word=term});
      if (retroVec != NULL) {
        WordVec* newWordVec = palloc0(sizeof(WordVec));
        newWordVec->vector = palloc0(sizeof(float)*dim*sizeof(float));
        memcpy(newWordVec->vector, retroVec->vector, dim);
        newWordVec->word = term; // palloc0(strlen(retroVec->word)+1);
        // sprintf(newWordVec->word, "%s", retroVec->word);
        newWordVec->id = retroVec->id;
        newWordVec->dim = retroVec->dim;
        hashmap_set(result, newWordVec);
      }
    }
  }
  return result;
}

void updateRetroVecs(struct hashmap* retroVecs, struct hashmap* newVecs) {
    // TODO: newVecs has values not in retroVecs???? -> Add
    l2IterStr* str = palloc(sizeof(l2IterStr));
    str->vecs = retroVecs;
    hashmap_scan(newVecs, iterUpdate, str);
}

bool iterL2(const void* item, void* data) {
    const WordVec* wv1 = item;
    WordVec* wv2;
    l2IterStr* str = data;

    if ((wv2 = (WordVec*)hashmap_get(str->vecs, &(WordVec){.word=wv1->word}))) {
        float distance = calcL2Norm(wv1->vector, wv2->vector, str->dim);
        // elog(INFO, "word %s distance %f", wv1->word, distance);
        str->delta += distance;
    }
    return true;
}

double calcDelta(struct hashmap* retroVecs, struct hashmap* newVecs, int dim) {
    l2IterStr str = {.vecs=retroVecs, .dim=dim, .delta=0};
    hashmap_scan(newVecs, iterL2, &str);
    return str.delta;
}

float calcL2Norm(float* vec1, float* vec2, int dim) {
    return sqrtf(squareDistance(vec1, vec2, dim));
}
