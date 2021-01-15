// clang-format off

#include "retrofitting.h"

#include "postgres.h"
#include "executor/spi.h"
#include "index_utils.h"
#include "math.h"
#include "stdlib.h"
#include "string.h"
#include "utils/builtins.h"
#include "utils/palloc.h"

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
    static RelationData ret[10];                    // TODO: size??
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
    DeltaElem* ret = palloc(t[k].size * sizeof(DeltaElem));                              // TODO: dynamic size
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
    DeltaCat* ret = palloc0(maxTok * sizeof(DeltaCat));                              // TODO: dynamic size
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
    DeltaRel* ret = palloc(maxTok * sizeof(DeltaRel));                              // TODO: dynamic size
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
//                (ret + i)->groups = palloc0(sizeof(DeltaRelElem) * ((ret + i)->groupCount + 1));          // TODO: remove

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
            substr1[i + 1] = '\0';

            char* substr2 = palloc0(len - i);
            memcpy(substr2, &str[i + 1], len - i - 1);
            substr2[len - i - 1] = '\0';

            strcat(substr1, "''");
            strcat(substr1, escape(substr2));
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
        sprintf(command, "DELETE FROM %s", *(tables + i));

        SPI_connect();
        ret = SPI_exec(command, 0);
        SPI_finish();
    }
    pfree(command);
}

void updateColumnStatistics(ColumnData* columnData, int columnCount) {
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

void updateRelationStatistics(struct RelationData* relationData, int count) {
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

void updateRelNumDataStatistics(RelNumData* relNumData, int relCount) {
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

WordVec* getWordVecs(char* tableName, int* size) {
    char* command;
    ResultInfo rInfo;
    float4* tmp;
    WordVec* result = NULL;

    if (SPI_connect() == SPI_OK_CONNECT) {
        command = palloc0(100);
        sprintf(command, "SELECT * FROM %s", tableName);
        rInfo.ret = SPI_execute(command, true, 0);
        rInfo.proc = SPI_processed;
        *size = rInfo.proc;
        result = SPI_palloc(rInfo.proc * sizeof(WordVec));

        if (rInfo.ret == SPI_OK_SELECT && SPI_tuptable != NULL) {
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            SPITupleTable *tuptable = SPI_tuptable;

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

                result[i].id = DatumGetInt32(id);
                result[i].word = SPI_palloc(strlen(word) + 1);
                snprintf(result[i].word, strlen(word) + 1, "%s", word);
                result[i].word[strlen(word)] = '\0';
                tmp = (float4 *) VARDATA(vectorData);
                result[i].vector = SPI_palloc(VARSIZE(vectorData) - VARHDRSZ);
                n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
                memcpy(result[i].vector, tmp, n * sizeof(float4));
                result[i].dim = n;
            }
        }
        SPI_finish();
    } else {
        elog(WARNING, "WordVecs request failed!");
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

float* calcColMean(char* tableName, char* column, char* vecTable, const char* tokenization, int dim) {
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

    char** pastTerms = palloc0(1000 * sizeof(char*));
    int pastTermCount = 0;

    SPI_connect();
    command = palloc0(500);
    sprintf(command, "SELECT regexp_replace(%s, '[\\.#~\\s\\xa0,\\(\\)/\\[\\]:]+', '_', 'g'), we.vector FROM %s "
                     "LEFT OUTER JOIN %s AS we ON regexp_replace(%s, '[\\.#~\\s\\xa0,\\(\\)/\\[\\]:]+', '_', 'g') = we.word",
                     tabCol, tableName, vecTable, tabCol);
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
                if (tmpVec == NULL) {
                    tmpVec = palloc(VARSIZE(vectorData) - VARHDRSZ);
                }
                n = (VARSIZE(vectorData) - VARHDRSZ) / sizeof(float4);
                memcpy(tmpVec, tmp, n * sizeof(float4));
            } else {
                term = palloc0(strlen(word) + 1);
                snprintf(term, strlen(word) + 1, "%s", word);
                tmpVec = inferVec(term, vecTable, "_", tokenization, dim);
                pfree(term);
            }

            if (!isZeroVec(tmpVec, dim)) {
                isEqual = 0;

                for (int i = 0; i < pastTermCount; ++i) {
                    if (strcmp(pastTerms[i], word) == 0) {
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
        }
    }
    SPI_finish();
    divVec(sum, count, dim);
    pfree(tabCol);
    return sum;
}

void insertColMeanToDB(char* tabCol, float* mean, const char* tokenization, int dim) {
    // TODO: insertion somehow not working
    char* command;
    char* cur;
    ResultInfo rInfo;

    if (SPI_connect() == SPI_OK_CONNECT) {
        command = palloc0(500 + dim * 15);
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
        sprintf(cur, "}'::float4[]), '%s')", tokenization);

        rInfo.ret = SPI_execute(command, false, 0);
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

float* getColMean(char* column, char* vecTable, const char* tokenization, int dim) {
    char** dbTokens = NULL;
    float* mean = NULL;

    dbTokens = getTableAndCol(column);
    char* tabCol = palloc0(strlen(dbTokens[0]) + strlen(dbTokens[1]) + 2);
    sprintf(tabCol, "%s.%s", dbTokens[0], dbTokens[1]);

    mean = getColMeanFromDB(tabCol, tokenization, dim);

    if (mean == NULL) {
        mean = calcColMean(dbTokens[0], dbTokens[1], vecTable, tokenization, dim);
        insertColMeanToDB(tabCol, mean, tokenization, dim);
    }
    pfree(tabCol);
    return mean;
}

float* calcCentroid(ProcessedRel* relation, char* retroVecTable, int dim) {
    float* sum = palloc0(dim * sizeof(float));

    SPI_connect();

    char** relKeys = getTableAndColFromRel(relation->key);
    char* selector;
    if (strcmp(relation->target, "col1") == 0) {
        selector = palloc0(strlen(relKeys[0]) + strlen(relKeys[1]) + 2);
        sprintf(selector, "%s.%s", relKeys[0], relKeys[1]);
    } else {
        selector = palloc0(strlen(relKeys[2]) + strlen(relKeys[3]) + 2);
        sprintf(selector, "%s.%s", relKeys[2], relKeys[3]);
    }

    char* tableName1 = relKeys[0];
    char* tableName2 = relKeys[2];


    char* command = palloc0(500);
    ResultInfo rInfo;
    float4* tmp;

    char* tab2WithoutS = palloc0(strlen(tableName2));           // TODO: doesnt work with categories -> category    --> find better solution
    strcpy(tab2WithoutS, tableName2);
    tab2WithoutS[strlen(tab2WithoutS) - 1] = '\0';

    sprintf(command, "SELECT vector FROM ("
                     "SELECT DISTINCT * "
                     "FROM %s AS rtv JOIN (SELECT regexp_replace(%s, '[\\.#~\\s\\u00a0,\\(\\)/\\[\\]:]+', '_', 'g') AS term "
                     "                      FROM %s INNER JOIN %s ON %s.%s_id = %s.id) as sub "
                     "ON rtv.word = CONCAT('%s#', sub.term) "
                     ") as res",
            retroVecTable, selector, tableName1, tableName2, tableName1, tab2WithoutS, tableName2, selector);
    // TODO: might not work with apps.name~genres.name:apps_genres
    // TODO: also maybe dynamic 'selector'
    // TODO: switch tabCol1 for which col is needed (1 or 2)

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

void updateCentroidInDB(ProcessedRel* relation, float* centroid, int dim) {         // TODO: no result in DB
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

        elog(INFO, "%s", command);
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

void getToks(char* s, const char* delim, char* tok1, char* tok2) {
    char cpy[2000];                                             // TODO: set dynamic ???
    memset(cpy, 0, 2000);
    strcpy(cpy, s);

    // TODO: set tok1, tok2 size dynamic
    strcpy(tok1, strtok(cpy, delim));
    strcpy(tok2, strtok(NULL, delim));
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
    char* col1 = palloc0(100);                  // TODO: set size dynamic on usage
    char* col2 = palloc0(100);
    char* tok1 = palloc0(1000);
    char* tok2 = palloc0(1000);
    char* key1 = palloc0(1100);
    char* key2 = palloc0(1100);

    for (int i = 0; i < deltaRelCount; i++) {
        getToks((deltaRel + i)->relation, delim, col1, col2);
        for (int j = 0; j < (deltaRel + i)->groupCount; ++j) {
            for (int k = 0; k < (deltaRel + i)->groups[j].elementCount; k++) {
                getToks((deltaRel + i)->groups[j].elements[k].name, delim, tok1, tok2);

                sprintf(key1, "%s#%s", col1, tok1);
                sprintf(key2, "%s#%s", col2, tok2);

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

void* addMissingVecs(WordVec* retroVecs, int* retroVecsSize, ProcessedDeltaEntry* processedDelta, int processedDeltaCount, const char* tokenizationStrategy) {
    int* vecsToAdd = palloc0(sizeof(int) * processedDeltaCount);
    int toAdd = 0;

    for (int i = 0; i < processedDeltaCount; i++) {
        int contained = 0;
        for (int j = 0; j < *retroVecsSize; j++) {
            if (strcmp(processedDelta[i].name, retroVecs[j].word) == 0) {
                contained = 1;
                break;
            }
        }
        if (!contained) {
            vecsToAdd[toAdd] = i;
            toAdd++;
        }
    }

    if (toAdd > 0) {
        char* originalTableName = palloc0(100);
        getTableName(ORIGINAL, originalTableName, 100);
        retroVecs = prealloc(retroVecs, sizeof(WordVec) * (*retroVecsSize + toAdd));
        for (int i = 0; i < toAdd; i++) {
            retroVecs[*retroVecsSize + i].word = processedDelta[vecsToAdd[i]].name;
            retroVecs[*retroVecsSize + i].vector = inferVec(processedDelta[vecsToAdd[i]].name, originalTableName, "_", tokenizationStrategy, retroVecs->dim);
            retroVecs[*retroVecsSize + i].id = -1;
            retroVecs[*retroVecsSize + i].dim = retroVecs->dim;
        }
        *retroVecsSize += toAdd;
        pfree(originalTableName);
    }
    pfree(vecsToAdd);
    return retroVecs;
}

float* inferVec(char* name, char* tableName, char* delimiter, const char* tokenizationStrategy, int dim) {
    char* tmp;
    char* term;
    int index = 0;
    char* wordVecTableDelimiter = "_";

    char* query;
    char* single;
    WordVec* wordVec;
    WordVec* lastMatchedVec = palloc(sizeof(WordVec));
    WordVec* lastMatchedSingleVec = NULL;
    lastMatchedVec->vector = palloc0(dim * sizeof(float));
    float* vec = palloc0(dim * sizeof(float4));
    float count = 0;
    int len;

    char** usedTokens = NULL;
    int usedTokensLength = 0;

    tmp = strchr(name, '#');
    if (tmp != NULL) {
        index = (int)(tmp - name);
        index = index ? index + 1 : 0;
    }
    term = palloc0(strlen(name) + index + 1);
    strcpy(term, name + index);
    query = palloc0(strlen(term) + 1);
    single = palloc0(strlen(term) + 1);

    for (char* p = strtok(term, delimiter); p != NULL; p = strtok(NULL, delimiter)) {
        strcpy(single, p);
        if (strlen(single) == 0) continue;
        len = strlen(query);
        if (len == 0) {
            strcpy(query, single);
        } else {
            strcpy(query + len, wordVecTableDelimiter);
            strcpy(query + len + 1, single);
        }

        wordVec = getWordVec(tableName, query);

        if (wordVec != NULL) {
            lastMatchedVec = wordVec;
        } else {
            wordVec = getWordVec(tableName, single);

            memset(query, '\0', strlen(query));
            strcpy(query, single);

            if (wordVec == NULL) {
                continue;
            }

            lastMatchedSingleVec = wordVec;

            if (lastMatchedVec != NULL && !isZeroVec(lastMatchedVec->vector, dim)) {
                if (strcmp(tokenizationStrategy, "log10") == 0) {
                    float log = (float)log10((double)lastMatchedVec->id);
                    multVecF(lastMatchedVec->vector, log, dim);
                    sumVecs(vec, lastMatchedVec->vector, dim);
                    count += log;
                } else {

                    usedTokens = prealloc(usedTokens, (usedTokensLength + 1) * sizeof(char*));
                    usedTokens[usedTokensLength] = palloc0(strlen(lastMatchedVec->word) + 1);
                    snprintf(usedTokens[usedTokensLength], strlen(lastMatchedVec->word) + 1, "%s", lastMatchedVec->word);
                    usedTokensLength++;


                    sumVecs(vec, lastMatchedVec->vector, dim);
                    count++;
                }
            }
            memcpy(lastMatchedVec, lastMatchedSingleVec, sizeof(WordVec));
            pfree(lastMatchedSingleVec);
            lastMatchedSingleVec = NULL;
        }
    }
    if (strlen(query) && lastMatchedVec != NULL && !isZeroVec(lastMatchedVec->vector, dim)) {
        if (strcmp(tokenizationStrategy, "log10") == 0) {
            float log = (float)log10((double)lastMatchedVec->id);
            multVecF(lastMatchedVec->vector, log, dim);
            sumVecs(vec, lastMatchedVec->vector, dim);
            count += log;
        } else {
            usedTokens = prealloc(usedTokens, (usedTokensLength + 1) * sizeof(char*));
            usedTokens[usedTokensLength] = palloc0(strlen(lastMatchedVec->word) + 1);
            snprintf(usedTokens[usedTokensLength], strlen(lastMatchedVec->word) + 1, "%s", lastMatchedVec->word);
            usedTokensLength++;


            sumVecs(vec, lastMatchedVec->vector, dim);
            count++;
        }
    }
    if (count != 0) {
        divVecF(vec, count, dim);
    } else {
        for (int i = 0; i < dim; i++) {
            vec[i] = 0;
        }
    }
    return vec;
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

char** getNTok(char* word, char* delim, int size) {             // TODO: integrate with getTok()
    char* s = palloc0(strlen(word) + 1);
    strcpy(s, word);
    char** result = palloc0(size * sizeof(char*));

    int i = 0;
    for (char* p = strtok(s, delim); p != NULL; p = strtok(NULL, delim)) {
        result[i] = palloc0(strlen(p) + 1);
        strcpy(result[i], p);
        i++;
    }
    return result;
}

char** getTableAndCol(char* word) {             // TODO: remove maybe
    char* s = palloc0(strlen(word) + 1);
    strcpy(s, word);
    char** result = palloc0(2 * sizeof(char*));
    result = getNTok(s, ".", 2);
    return result;
}

char** getTableAndColFromRel(char* word) {
    char* s = palloc0(strlen(word) + 1);
    strcpy(s, word);
    char** result = palloc0(4 * sizeof(char*));
    char** tmp = palloc0(2 * sizeof(char*));

    char** tabs = getNTok(word, "~", 2);
    result = getNTok(tabs[0], ".", 2);
    tmp = getNTok(tabs[1], ":", 2);
    tmp = getNTok(tmp[0], ".", 2);

    result[2] = tmp[0];
    result[3] = tmp[1];

    return result;
}

WordVec* calcRetroVecs(ProcessedDeltaEntry* processedDelta, int processedDeltaCount, WordVec* retroVecs, int retroVecsSize, RetroConfig* retroConfig, int* newVecsSize) {
    ProcessedDeltaEntry entry;
    ProcessedRel relation;
    int dim = retroVecs[0].dim;
    char* query = palloc0(100);

    float* col_mean;
    float localeBeta;
    float* nominator = palloc0(dim * sizeof(float4));
    float denominator;

    float* centroid = palloc0(dim * sizeof(float4));
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

    float* currentVec = palloc0(dim * sizeof(float4));

    WordVec* result = palloc(processedDeltaCount * sizeof(WordVec));

    char* originalTableName = palloc0(100);
    char* retroVecTable = palloc0(100);

    getTableName(ORIGINAL, originalTableName, 100);
    getTableName(RETRO_VECS, retroVecTable, 100);

    char** tmp = palloc0(2 * sizeof(void*));


    for (int i = 0; i < processedDeltaCount; i++) {
        entry = processedDelta[i];
        // TODO: ColMean is still a bit of. E.g. for apps.name usage of 189 elements and not 180
        col_mean = getColMean(entry.column, originalTableName, retroConfig->tokenization, dim);
        localeBeta = (float)retroConfig->beta / (entry.relationCount + 1);

        currentVec = inferVec(entry.name, originalTableName, "_", retroConfig->tokenization, dim);
        memcpy(nominator, currentVec, dim * sizeof(float4));
        multVec(nominator, retroConfig->alpha, dim);

        sumVecs(nominator, col_mean, dim);
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
            multVecF(delta, retroConfig->delta * 2 / (max_c * (max_r + 1)), dim);
            subVecs(nominator, delta, dim);
            sprintf(query, "SELECT size FROM rel_col_stats WHERE id = (SELECT %s FROM relation_stats WHERE relation_name = '%s')",
                    relation.target, relation.key);
            size = getIntFromDB(query);
            denominator -= (float)retroConfig->delta * 2 / (max_c * (max_r + 1)) * size;

            for (int k = 0; k < relation.termCount; k++) {
                term = relation.terms[k];
                sprintf(query, "SELECT value FROM rel_num_stats WHERE rel = '%s'", escape(term));           // TODO: should be more efficient to go over ids or something like that
                value = getIntFromDB(query);                // TODO: test for error e.g. -1

                tmp = getNTok(term, "#", 2);
                sprintf(query, "SELECT cardinality FROM cardinality_stats "
                               "JOIN rel_col_stats ON cardinality_stats.col_id = rel_col_stats.id "
                               "WHERE rel_col_stats.name = '%s' AND value = '%s'",
                               escape(tmp[0]), escape(tmp[1]));
                cardinality = getIntFromDB(query);              // TODO: test for error e.g. -1
                gamma_inv = (float)retroConfig->gamma / (cardinality * (value + 1));

                for (int l = 0; l < retroVecsSize; l++) {
                    if (strcmp(term, retroVecs[l].word) == 0) {
                        memcpy(retroVec, retroVecs[l].vector, dim * sizeof(float4));
                        break;
                    }
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

            elog(INFO, "after for loop");
        }
        elog(INFO, "after 2nd for loop");
        result[i].word = entry.name;
        result[i].vector = palloc(dim * sizeof(float4));
        memcpy(result[i].vector, nominator, dim * sizeof(float4));
        divVec(result[i].vector, denominator, dim);
    }
    return result;
}

void updateRetroVecs(WordVec* retroVecs, int retroVecsSize, WordVec* newVecs, int newVecsSize) {
    // TODO: newVecs has values not in retroVecs???? -> Add
    for (int j = 0; j < newVecsSize; j++) {
        for (int k = 0; k < retroVecsSize; ++k) {
            if (strcmp(newVecs[j].word, retroVecs[k].word) == 0) {          // TODO: compairing ids should be faster -> check if consistent
                retroVecs[k].vector = newVecs[j].vector;
                break;
            }
        }
    }
}

float calcDelta(WordVec* retroVecs, int retroVecsSize, WordVec* newVecs, int newVecsSize) {
    float delta = 0;
    int dim = retroVecs[0].dim;
    for (int j = 0; j < newVecsSize; j++) {
        for (int k = 0; k < retroVecsSize; ++k) {
            if (strcmp(newVecs[j].word, retroVecs[k].word) == 0) {          // TODO: compairing ids should be faster -> check if consistent
                delta += calcL2Norm(retroVecs[k].vector, newVecs[j].vector, dim);
                break;
            }
        }
    }
    return delta;
}

float calcL2Norm(float* vec1, float* vec2, int dim) {
    return sqrtf(squareDistance(vec1, vec2, dim));
}
