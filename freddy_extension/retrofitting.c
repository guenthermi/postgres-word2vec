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
    long fsize;
    jsmntok_t* t = NULL;

    fp = fopen(path, "r");
    if (fp == NULL) {
        return t;
    }
    fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    rewind(fp);

    *json = palloc0(fsize + 1);
    fread(*json, 1, fsize, fp);
    fclose(fp);

    jsmn_parser p;
    jsmn_init(&p);

    *r = jsmn_parse(&p, *json, strlen(*json), NULL, 0);
    t = palloc(*r * sizeof(*t));

    jsmn_init(&p);
    *r = jsmn_parse(&p, *json, strlen(*json), t, *r);
    return t;
}

char* sprintf_json(const char* json, jsmntok_t* t) {
    char* s = palloc0(t->end - t->start + 1);
    sprintf(s, "%.*s", t->end - t->start, json + t->start);
    return s;
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

RelationData* getRelationData(const char* json, jsmntok_t* t, int k, int* relationDataSize) {
    RelationData* ret = palloc0(sizeof(RelationData) * t[k].size);
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

DeltaCat* getDeltaCats(const char* json, jsmntok_t* t, int maxTok, int* count, int* lastIndex) {
    DeltaCat* ret = palloc0(maxTok * sizeof(DeltaCat));
    int index = 1;
    int elemSize = 0;
    int infElemSize = 0;
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
            *count += 1;
        }
        if (t[index + 1].type == JSMN_ARRAY) break;
        index += 12 + elemSize + infElemSize;
    }
    *lastIndex = index;
    return ret;
}

DeltaRel* getDeltaRels(const char* json, jsmntok_t* t, int maxTok, int* count, int start) {
    DeltaRel* ret = palloc0(maxTok * sizeof(DeltaRel));
    int index = start;
    int elemSize = 0;
    *count = 0;

    for (int i = 0; index < maxTok; i++) {
        if (t[index + 1].type == JSMN_ARRAY) {
            (ret + i)->relation = sprintf_json(json, &t[index]);
            (ret + i)->groupCount = 0;
            index += 2;

            int size = t[index - 1].size;
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
                (ret + i)->groupCount += 1;
                index += 6 + elemSize + 1;
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
            char* substr1 = palloc0(len + 1);
            memcpy(substr1, str, i);

            char* substr2 = palloc0(len - i);
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
    int ret;
    char tables[5][20] = {"cardinality_stats", "column_stats", "rel_num_stats", "relation_stats", "rel_col_stats"};

    for (int i = 0; i < 5; i++) {
        sprintf(command, "TRUNCATE %s CASCADE", *(tables + i));

        SPI_connect();
        ret = SPI_exec(command, 0);
        SPI_finish();
    }
    pfree(command);
}

void insertColumnStatistics(ColumnData* columnData, int columnCount) {
    char* command;
    char* cur;
    int ret;

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
        ret = SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

void updateCardinalityStatistics(struct CardinalityData* cardinalityData, int count, char* relColId) {
    char* command;
    char* cur;
    int ret;

    for (int i = 0; i < count; i++) {
        command = palloc(100
                         + strlen((cardinalityData + i)->value)
                         + 10);
        cur = command;
        cur += sprintf(
                cur, "INSERT INTO cardinality_stats (value, cardinality, col_id) VALUES ('%s', %d, %s) ",
                escape((cardinalityData + i)->value), (cardinalityData + i)->cardinality, relColId);
        SPI_connect();
        ret = SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

char* updateRelColStatistics(struct RelationColumnData* relationColumnData) {
    char* command;
    char* cur;
    char* id = NULL;
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
        for (int i = 0; i < rInfo.proc; i++) {
            HeapTuple tuple = tuptable->vals[i];
            id = SPI_getvalue(tuple, tupdesc, 1);
        }
        SPI_finish();
    }
    if (id) {
        updateCardinalityStatistics(relationColumnData->cardinalities, relationColumnData->size, id);
    }
    pfree(command);
    return id;
}

void insertRelationStatistics(struct RelationData* relationData, int count) {
    char* col1;
    char* col2;
    char* command;
    char* cur;
    int ret;

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
        ret = SPI_exec(command, 0);
        SPI_finish();
        pfree(command);
    }
}

void insertRelNumDataStatistics(RelNumData* relNumData, int relCount) {
    char* command;
    char* cur;
    int ret;

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
        ret = SPI_exec(command, 0);
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

struct hashmap* getWordVecs(char* tableName, int* dim) {
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
                wv = SPI_palloc(sizeof(WordVec));

                HeapTuple tuple = tuptable->vals[i];
                id = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);
                word = (char*) SPI_getvalue(tuple, tupdesc, 2);
                vector = SPI_getbinval(tuple, tupdesc, 3, &rInfo.info);
                vectorData = DatumGetByteaP(vector);

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
        result = SPI_palloc(sizeof(WordVec));
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

        HeapTuple tuple = tuptable->vals[0];
        table = SPI_getvalue(tuple, tupdesc, 1);
        col = SPI_getvalue(tuple, tupdesc, 2);
        fTable = SPI_getvalue(tuple, tupdesc, 3);
        fCol = SPI_getvalue(tuple, tupdesc, 4);

        int length = strlen(table) + strlen(col) + strlen(fTable) + strlen(fCol) + 5 + 1;
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

void deleteRetroVecsDB(const char* tableName) {
    ResultInfo rInfo;
    char* command = palloc0(100 + strlen(tableName));
    sprintf(command, "TRUNCATE TABLE %s RESTART IDENTITY", tableName);
    if (SPI_connect() == SPI_OK_CONNECT) {
        rInfo.ret = SPI_exec(command, 0);
        SPI_finish();
    }
}

void retroVecsToDB(const char* tableName, struct hashmap* retroVecs, int dim) {     // TODO: do batchwise??
    // TODO: check if table exists and create if necessary???
    ResultInfo rInfo;
    retroQuery* query = palloc(sizeof(retroQuery));
    char* insertedRows;

    if (SPI_connect() == SPI_OK_CONNECT) {
        int queryLength = 100;
        hashmap_scan(retroVecs, getRetroQuerySize, &queryLength);

        elog(INFO, "writing retro vecs to database table %s", tableName);
        query->command = palloc0(queryLength);
        query->cur = query->command;
        query->cur += sprintf(query->cur, "INSERT INTO %s (word, vector) VALUES ", tableName);
        hashmap_scan(retroVecs, buildRetroQuery, query);
        query->command[strlen(query->command) - 2] = ';';
        query->command[strlen(query->command) - 1] = 0;

        rInfo.ret = SPI_exec(query->command, 0);
        rInfo.proc = SPI_processed;
        if (rInfo.ret != SPI_OK_INSERT) {
            elog(WARNING, "Failed to insert retrofitted vectors!");
        } else if (rInfo.proc > 0 && SPI_tuptable != NULL) {
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            SPITupleTable *tuptable = SPI_tuptable;
            HeapTuple tuple = tuptable->vals[0];
            insertedRows = SPI_getvalue(tuple, tupdesc, 2);

            elog(INFO, "%s retro vecs inserted!", insertedRows);        // TODO: alter for update
        }
        SPI_finish();
    }
}

float* calcColMean(char* tableName, char* column, char* vecTable, RadixTree* vecTree, const char* tokenization, int dim) {
    // TODO: not exactly right but very close
    char* command;
    ResultInfo rInfo;
    float4* tmp;
    float* tmpVec = NULL;

    char* tabCol = palloc0(strlen(tableName) + strlen(column) + 2);
    sprintf(tabCol, "%s.%s", tableName, column);

    float* sum = palloc0(dim * sizeof(float));
    int count = 0;
    char* term;

    int isEqual = 0;

    char** pastTerms = palloc0(1000 * sizeof(char*));       // TODO: remove magic number
    int pastTermCount = 0;

    SPI_connect();
    command = palloc0(500);
    sprintf(command, "SELECT regexp_replace(%s, '[\\.#~\\s\\xa0,\\(\\)/\\[\\]:]+', '_', 'g'), we.vector FROM %s "
                     "LEFT OUTER JOIN %s AS we ON regexp_replace(%s, '[\\.#~\\s\\xa0,\\(\\)/\\[\\]:]+', '_', 'g') = we.word",
                     tabCol, tableName, vecTable, tabCol);      // TODO: maybe remove JOIN and do everything with vecTree
    rInfo.ret = SPI_exec(command, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;

        for (int i = 0; i < rInfo.proc; i++) {
            char* word;
            Datum vector;
            int n = 0;
            bytea* vectorData;

            HeapTuple tuple = tuptable->vals[i];
            word = SPI_getvalue(tuple, tupdesc, 1);
            vector = SPI_getbinval(tuple, tupdesc, 2, &rInfo.info);

            if (rInfo.info == 0) {              // no NULL value at vector
                vectorData = DatumGetByteaP(vector);
                tmp = (float4 *) VARDATA(vectorData);
                tmpVec = palloc0(VARSIZE(vectorData) - VARHDRSZ);           // TODO: improve performance: dont allocate every run!
                n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
                memcpy(tmpVec, tmp, n * sizeof(float4));
            } else {
                term = palloc0(strlen(word) + 1);
                snprintf(term, strlen(word) + 1, "%s", word);
                tmpVec = inferVec(term, vecTree, "_", tokenization, dim);
                pfree(term);
            }

            if (!isZeroVec(tmpVec, dim)) {
                isEqual = 0;

                for (int j = 0; j < pastTermCount; ++j) {           // TODO: needed?
                    if (strcmp(pastTerms[j], word) == 0) {
                        isEqual = 1;
                        break;
                    }
                }

                if (isEqual) continue;

                sumVecs(sum, tmpVec, dim);
                count++;
            }

            pastTerms[pastTermCount] = palloc0(strlen(word) + 1);
            snprintf(pastTerms[pastTermCount], strlen(word) + 1, "%s", word);
            pastTermCount++;
            pfree(tmpVec);
        }
    }
    SPI_finish();
    divVec(sum, count, dim);
    pfree(tabCol);
    return sum;
}

void insertColMeanToDB(char* tabCol, float* mean, const char* tokenization, int dim) {
    char* command;
    char* cur;
    ResultInfo rInfo;

    if (SPI_connect() == SPI_OK_CONNECT) {
        command = palloc0(500 + dim * (sizeof(float) + 2));
        cur = command;
        cur += sprintf(cur,
                       "INSERT INTO col_mean (column_id, mean, tokenization_strat) VALUES ((SELECT id FROM column_stats WHERE name = '%s'), vec_to_bytea('{",
                       tabCol);
        for (int i = 0; i < dim; i++) {
            if (i < dim - 1) {
                cur += sprintf(cur, "%f, ", mean[i]);
            } else {
                cur += sprintf(cur, "%f", mean[i]);
            }
        }
        sprintf(cur, "}'::float4[]), '%s');", tokenization);

        rInfo.ret = SPI_exec(command, 0);
        if (rInfo.ret != SPI_OK_INSERT) {
            elog(WARNING, "Failed to insert col mean vector!");
        }
        rInfo.proc = SPI_processed;
        SPI_finish();
    }
}

float* getColMeanFromDB(char* tabCol, const char* tokenization, int dim) {
    // TODO: no option to recalculate mean
    char* command;
    ResultInfo rInfo;

    float* result = NULL;

    SPI_connect();
    command = palloc0(500);
    sprintf(command, "SELECT col_mean.mean FROM col_mean JOIN column_stats ON "
                     "col_mean.column_id = column_stats.id WHERE column_stats.name = '%s' AND col_mean.tokenization_strat = '%s'",
            tabCol, tokenization);
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
        vectorData = DatumGetByteaP(vector);

        tmp = (float4 *) VARDATA(vectorData);
        result = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
        n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
        memcpy(result, tmp, n * sizeof(float4));
    }
    SPI_finish();
    return result;
}

float* getColMean(char* column, char* vecTable, RadixTree* vecTree, const char* tokenization, int dim) {
    char** dbTokens = NULL;
    float* mean = NULL;

    dbTokens = getTableAndCol(column);
    char* tabCol = palloc0(strlen(dbTokens[0]) + strlen(dbTokens[1]) + 2);
    sprintf(tabCol, "%s.%s", dbTokens[0], dbTokens[1]);

    mean = getColMeanFromDB(tabCol, tokenization, dim);

    if (mean == NULL) {
        mean = calcColMean(dbTokens[0], dbTokens[1], vecTable, vecTree, tokenization, dim);
        insertColMeanToDB(tabCol, mean, tokenization, dim);
    }
    pfree(tabCol);
    return mean;
}

float* calcCentroid(ProcessedRel* relation, char* retroVecTable, int dim) {
    float* sum = palloc0(dim * sizeof(float));

    SPI_connect();
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

    rInfo.ret = SPI_exec(command, 0);
    rInfo.proc = SPI_processed;

    if (rInfo.ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;

        for (int i = 0; i < rInfo.proc; i++) {
            Datum vector;
            int n = 0;
            bytea* vectorData;

            HeapTuple tuple = tuptable->vals[i];
            vector = SPI_getbinval(tuple, tupdesc, 1, &rInfo.info);

            if (rInfo.info == 0) {              // no NULL value at vector
                vectorData = DatumGetByteaP(vector);
                tmp = (float4 *) VARDATA(vectorData);
                n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
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

    command = palloc0(500);
    sprintf(command, "SELECT centroid_bin FROM relation_stats JOIN rel_col_stats ON relation_stats.%s = rel_col_stats.id WHERE relation_stats.relation_name = '%s'",
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
        result[*index].vector = elements[i].value;

        result[*index].column = palloc0(strlen(deltaCat->name) + 1);
        result[*index].column = deltaCat->name;

        result[*index].relationCount = 0;
        *index += 1;
    }
}

void addPrecessRel(ProcessedDeltaEntry* elem, DeltaRelElem* deltaRelElem, char* relation, char* term, char* target) {
    int added = 0;
    char* relKey = palloc0(strlen(relation) + strlen(deltaRelElem->name) + 2);
    sprintf(relKey, "%s:%s", relation, deltaRelElem->name);

    for (int l = 0; l < elem->relationCount; l++) {
        if (strcmp(relKey, (elem->relations + l)->key) == 0) {
            (elem->relations + l)->terms = prealloc((elem->relations + l)->terms, sizeof(char*) * ((elem->relations + l)->termCount + 1));
            (elem->relations + l)->terms[(elem->relations + l)->termCount] = palloc0(strlen(term) + 1);
            memcpy((elem->relations + l)->terms[(elem->relations + l)->termCount], term, strlen(term));
            (elem->relations + l)->termCount += 1;
            added = 1;
            break;
        }
    }

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
    int currentCount = 0;
    int newCount = 0;
    ProcessedDeltaEntry* result = NULL;

    int index = 0;
    for (int i = 0; i < deltaCatCount; i++) {
        newCount = currentCount +  (deltaCat + i)->elementCount + (deltaCat + i)->inferredElementCount;
        if (newCount == currentCount) continue;
        if (result == NULL) {
            result = palloc(sizeof(ProcessedDeltaEntry) * newCount);
        }
        result = prealloc(result, sizeof(ProcessedDeltaEntry) * newCount);
        currentCount = newCount;

        processDeltaElements(result, (deltaCat + i), (deltaCat + i)->elementCount, (deltaCat + i)->elements, &index);
        processDeltaElements(result, (deltaCat + i), (deltaCat + i)->inferredElementCount, (deltaCat + i)->inferredElements, &index);
    }

    *count = currentCount;

    char* delim = "~";
    char* key1;
    char* key2;
    char** cols = palloc0(2 * sizeof(char*));
    char** toks = palloc0(2 * sizeof(char*));

    for (int i = 0; i < deltaRelCount; i++) {
        cols = getNToks((deltaRel + i)->relation, delim, 2);
        for (int j = 0; j < (deltaRel + i)->groupCount; ++j) {
            for (int k = 0; k < (deltaRel + i)->groups[j].elementCount; k++) {
                toks = getNToks((deltaRel + i)->groups[j].elements[k].name, delim, 2);

                key1 = palloc0(strlen(cols[0]) + strlen(toks[0]) + 2);
                key2 = palloc0(strlen(cols[1]) + strlen(toks[1]) + 2);
                sprintf(key1, "%s#%s", cols[0], toks[0]);
                sprintf(key2, "%s#%s", cols[1], toks[1]);

                for (int l = 0; l < currentCount; l++) {
                    if (strcmp(result[l].name, key1) == 0) {
                        addPrecessRel(&result[l], &(deltaRel + i)->groups[j], (deltaRel + i)->relation, key2, "col2");
                    }
                    if (strcmp(result[l].name, key2) == 0) {
                        addPrecessRel(&result[l], &(deltaRel + i)->groups[j], (deltaRel + i)->relation, key1, "col1");
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
            wv->vector = inferVec(processedDelta[i].name, vecTree, "_", tokenizationStrategy, dim);
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
    char* split;
    void* el = (void*)1;

    RadixTree* current;
    RadixTree* old;
    RadixTree* foundTree;

    t = palloc0(strlen(vec->word) + 1);
    snprintf(t, strlen(vec->word) + 1, "%s", vec->word);
    current = tree;

    for (char* s = strtok(t, delimiter); s != NULL; s = strtok(NULL, delimiter)) {
        split = palloc0(strlen(s) + 1);
        snprintf(split, strlen(s) + 1, "%s", s);
        foundTree = NULL;
        if (current->children != NULL) {
            foundTree = hashmap_get(current->children, &(RadixTree){.value=split});
        }

        if (foundTree != NULL) {
            old = current;
            current = foundTree;
        } else {
            if (current->children == NULL) {
                current->children = hashmap_new(sizeof(RadixTree), 0, 0, 0, radixHash, radixCompare, NULL);
            }

            RadixTree* new = palloc(sizeof(RadixTree));
            new->id = vec->id;
            new->value = split;
            new->vector = NULL;
            new->children = NULL;

            el = hashmap_set(current->children, new);
            if (!el && hashmap_oom(current->children)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DIVISION_BY_ZERO),
                        errmsg("hash map out of memory")));
            }

            new = hashmap_get(current->children, &(RadixTree){.value=split});
            if (!new) {
                ereport(ERROR,
                    (errcode(ERRCODE_DIVISION_BY_ZERO),
                        errmsg("radix tree entry not found")));
            }
            old = current;
            current = new;
        }
    }
    current->vector = vec->vector;
    pfree(t);
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
    float log;
    float* tmpVec = palloc0(dim * sizeof(float));


    if (strcmp(tokenizationStrategy, "log10") == 0) {
        log = (float)log10((double)tree->id);
        memcpy(tmpVec, tree->vector, dim * sizeof(float));
        multVecF(tmpVec, log, dim);
        sumVecs(result, tmpVec, dim);
        *tokens += log;
    } else {
        sumVecs(result, tree->vector, dim);
        *tokens += 1;
    }
    pfree(tmpVec);
}

float* inferVec(char* term, RadixTree* tree, char* delimiter, const char* tokenizationStrategy, int dim) {
    char* tmp;
    int index = 0;

    RadixTree* foundTree = NULL;
    RadixTree* lastMatch = NULL;
    RadixTree* current = tree;

    float* result = palloc0(dim * sizeof(float));
    int tokens = 0;

    tmp = strchr(term, '#');
    if (tmp != NULL) {
        index = (int)(tmp - term);
        index = index ? index + 1 : 0;
    }
    char* t = palloc0(strlen(term) + index + 1);
    char* ot = t;
    strcpy(t, term + index);

    int strSize = strlen(t);
    char* rest = palloc0(strSize + 1);

    for (char* split = strtok_r(t, delimiter, &t); split != NULL;) {
        foundTree = NULL;
        if (current->children) {
            foundTree = hashmap_get(current->children, &(RadixTree){.value=split});
        }
        if (foundTree) {
            current = foundTree;
            if (current->vector) {
                lastMatch = current;
                if (t) {
                    snprintf(rest, strlen(t) + 1, "%s", t);
                }
            }
        } else {
            current = tree;
        }
        split = strtok_r(t, delimiter, &t);

        if (split == NULL && strlen(rest)) {
            inferAdd(result, lastMatch, &tokens, tokenizationStrategy, dim);
            current = tree;
            memset(ot, '\0', strSize);
            t = ot;
            snprintf(t, strlen(rest) + 1, "%s", rest);
            memset(rest, '\0', strSize);
            split = strtok_r(t, delimiter, &t);
        }
    }

    if (lastMatch && lastMatch != tree) {
        inferAdd(result, lastMatch, &tokens, tokenizationStrategy, dim);
    }

    divVec(result, tokens, dim);
    pfree(ot);
    return result;
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

char** getNToks(char* word, char* delim, int size) {
    char* s = palloc0(strlen(word) + 1);
    strncpy(s, word, strlen(word));
    char** result = palloc0(size * sizeof(char*));

    int i = 0;
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
    strcpy(s, word);
    char** result = palloc0(2 * sizeof(char*));
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

struct hashmap* calcRetroVecs(ProcessedDeltaEntry* processedDelta, int processedDeltaCount, struct hashmap* retroVecs, RetroConfig* retroConfig, RadixTree* vecTree, int dim) {
    ProcessedDeltaEntry entry;
    ProcessedRel relation;
    int querySize = 1000;
    char* query = palloc0(querySize);

    float* col_mean;
    float localeBeta;
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
    char* term;
    int value;
    void* el = (void*)1;

    float* currentVec = palloc0(dim * sizeof(float4));

    WordVec* wv;
    struct hashmap* result = hashmap_new(sizeof(WordVec), processedDeltaCount, 0, 0, wordVecHash, wordVecCompare, NULL);

    char* originalTableName = palloc0(100);
    char* retroVecTable = palloc0(100);

    getTableName(ORIGINAL, originalTableName, 100);
    getTableName(RETRO_VECS, retroVecTable, 100);

    char** tmp;


    for (int i = 0; i < processedDeltaCount; i++) {
        entry = processedDelta[i];
        // TODO: ColMean is still a bit of. E.g. for apps.name usage of 189 elements and not 180
        col_mean = getColMean(entry.column, originalTableName, vecTree, retroConfig->tokenization, dim);
        localeBeta = (float)retroConfig->beta / (entry.relationCount + 1);

        currentVec = inferVec(entry.name, vecTree, "_", retroConfig->tokenization, dim);
        memcpy(nominator, currentVec, dim * sizeof(float4));
        pfree(currentVec);
        multVec(nominator, retroConfig->alpha, dim);

        sumVecs(nominator, col_mean, dim);
        pfree(col_mean);
        add2Vec(nominator, localeBeta, dim);
        denominator = retroConfig->alpha + localeBeta;

        for (int j = 0; j < entry.relationCount; j++) {
            relation = entry.relations[j];
            gamma = (float)retroConfig->gamma / (relation.termCount * (entry.relationCount + 1));

            // TODO:
            // TODO: 'apps.name~genres.name:apps_genres' vs 'genres.name~apps.name:apps_genres'
            // TODO:

            sprintf(query, "SELECT max_r FROM relation_stats WHERE relation_name = '%s'", relation.key);
            max_r = getIntFromDB(query);
            sprintf(query, "SELECT max_c FROM relation_stats WHERE relation_name = '%s'", relation.key);
            max_c = getIntFromDB(query);

            // TODO: target column seems to be wrong!!!
            centroid = getCentroid(&relation, retroVecTable, dim);

            memcpy(delta, centroid, dim * sizeof(float4));
            pfree(centroid);
            multVecF(delta, (float)retroConfig->delta * 2 / (max_c * (max_r + 1)), dim);
            subVecs(nominator, delta, dim);
            resizeQuery(query, &querySize, strlen(relation.target) + strlen(relation.key) + 200);
            sprintf(query, "SELECT size FROM rel_col_stats WHERE id = (SELECT %s FROM relation_stats WHERE relation_name = '%s')",
                    relation.target, relation.key);
            size = getIntFromDB(query);
            denominator -= (float)retroConfig->delta * 2 / (max_c * (max_r + 1)) * size;

            for (int k = 0; k < relation.termCount; k++) {
                term = relation.terms[k];
                resizeQuery(query, &querySize, strlen(escape(term)) + 100);
                sprintf(query, "SELECT value FROM rel_num_stats WHERE rel = '%s'", escape(term));
                // TODO: should be more efficient to go over ids or something like that   ---> not possible now, because ids are auto increment on insertion. Ids would need to be inserted
                value = getIntFromDB(query);

                if (value == -1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("value not found in DB")));
                }

                tmp = getNToks(term, "#", 2);
                sprintf(query, "SELECT cardinality FROM cardinality_stats "
                               "JOIN rel_col_stats ON cardinality_stats.col_id = rel_col_stats.id "
                               "WHERE rel_col_stats.name = '%s' AND value = '%s'",
                        escape(tmp[0]), escape(tmp[1]));
                pfree(tmp[0]);
                pfree(tmp[1]);
                pfree(tmp);
                cardinality = getIntFromDB(query);
                if (cardinality == -1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("cardinality not found in DB")));
                }

                gamma_inv = (float) retroConfig->gamma / (cardinality * (value + 1));

                WordVec* rwv = hashmap_get(retroVecs, &(WordVec) {.word=term});
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
        wv = palloc(sizeof(WordVec));
        wv->word = entry.name;
        wv->vector = palloc0(dim * sizeof(float4));
        memcpy(wv->vector, nominator, dim * sizeof(float4));
        divVec(wv->vector, denominator, dim);
        el = hashmap_set(result, wv);
        if (!el && hashmap_oom(result)) {
            ereport(ERROR,
               (errcode(ERRCODE_DIVISION_BY_ZERO),
                    errmsg("hash map out of memory")));
        }
    }

    pfree(nominator);
    pfree(delta);
    pfree(retroVec);
    pfree(tmpVec);
    pfree(query);
    pfree(originalTableName);
    pfree(retroVecTable);

    return result;
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
        str->delta += calcL2Norm(wv1->vector, wv2->vector, str->dim);
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
