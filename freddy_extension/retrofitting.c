// clang-format off

#include "retrofitting.h"

#include "postgres.h"
#include "stdlib.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "index_utils.h"

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
                ret[i].size = sprintf_json(json, &t[index + 5]);                    // TODO:  should be number
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
            (ret + i)->cardinality = sprintf_json(json, &t[index + 1]);
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
        ret->size = sprintf_json(json, &t[k + 6]);
    }
    if (jsoneq(json, &t[k + 7], "cardinalities") == 0) {
        ret->cardinalities = getCardinalities(json, t, k + 8, strtol(ret->size, NULL, 10), &cardinalities);
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
                ret[i].max_r = sprintf_json(json, &t[index + countCol1 + countCol2 + 9]);
            }
            if (jsoneq(json, &t[index + countCol1 + countCol2 + 10], "max_c") == 0) {
                ret[i].max_c = sprintf_json(json, &t[index + countCol1 + countCol2 + 11]);
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
            ret[i].value = sprintf_json(json, &t[index + 1]);               // TODO:  should be number
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
    DeltaCat* ret = palloc(10 * sizeof(DeltaCat));                              // TODO: dynamic size
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
            }
            if (jsoneq(json, &t[index + 8 + elemSize], "query") == 0) {
                (ret + i)->query = sprintf_json(json, &t[index + 9 + elemSize]);
            }
            if (jsoneq(json, &t[index + 10 + elemSize], "inferred_elements") == 0) {
                (ret + i)->inferredElements = getDeltaElems(json, t, index + 11 + elemSize, &infElemSize);
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
    DeltaRel* ret = palloc(10 * sizeof(DeltaRel));                              // TODO: dynamic size
    int index = start;
    int elemSize = 0;
    *count = 0;

    for (int i = 0; index < maxTok; i++) {
        if (t[index + 1].type == JSMN_ARRAY) {
            (ret + i)->relation = sprintf_json(json, &t[index]);

            if (jsoneq(json, &t[index + 3], "name") == 0) {
                (ret + i)->name = sprintf_json(json, &t[index + 4]);
            }
            if (jsoneq(json, &t[index + 5], "type") == 0) {
                (ret + i)->type = sprintf_json(json, &t[index + 6]);
            }
            if (jsoneq(json, &t[index + 7], "elements") == 0) {
                (ret + i)->elements = getDeltaElems(json, t, index + 8, &elemSize);
            }
            *count += 1;
            index += 8 + elemSize + 1;
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
    char* command = palloc(100);
    int ret;
    char tables[5][20] = {"cardinality_stats", "column_stats", "rel_num_stats", "relation_stats", "rel_col_stats"};

    for (int i = 0; i < 5; i++) {
        sprintf(command, "DELETE FROM %s", *(tables + i));

        SPI_connect();
        ret = SPI_exec(command, 0);
        if (ret > 0) {
            SPI_finish();
        }
    }
    pfree(command);
}

void updateColumnStatistics(ColumnData* columnData, int columnCount) {
    // NOTE: needed to set max_stack_depth to 4 MB for it to work, maybe optimize...                                    // TODO: maybe back to 2MB??
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
                cur, "INSERT INTO column_stats (name, mean, size) VALUES ('%s', '%s', %s) "
                     "ON CONFLICT (name) DO UPDATE SET (mean, size) = (EXCLUDED.mean, EXCLUDED.size)",
                (columnData + i)->name, (columnData + i)->mean, (columnData + i)->size);
        SPI_connect();
        ret = SPI_exec(command, 0);
        if (ret > 0) {
            SPI_finish();
        }
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
                cur, "INSERT INTO cardinality_stats (value, cardinality, col_id) VALUES ('%s', %s, %s) ",
                escape((cardinalityData + i)->value), (cardinalityData + i)->cardinality, relColId);
        SPI_connect();
        ret = SPI_exec(command, 0);
        if (ret > 0) {
            SPI_finish();
        }
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
            cur, "INSERT INTO rel_col_stats (name, centroid, size) VALUES ('%s', '%s', %s) "
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
        updateCardinalityStatistics(relationColumnData->cardinalities, strtol(relationColumnData->size, NULL, 10), id);
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
                cur, "INSERT INTO relation_stats (relation_name, name, col1, col2, max_r, max_c) VALUES ('%s', '%s', %s, %s, %s, %s) "
                     "ON CONFLICT (relation_name) DO UPDATE SET (name, col1, col2, max_r, max_c) = (EXCLUDED.name, EXCLUDED.col1, EXCLUDED.col2, EXCLUDED.max_r, EXCLUDED.max_c)",
                escape((relationData + i)->relation_name), escape((relationData + i)->name), col1, col2, (relationData + i)->max_r, (relationData + i)->max_c);
        SPI_connect();
        ret = SPI_exec(command, 0);
        if (ret > 0) {
            SPI_finish();
        }
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
                cur, "INSERT INTO rel_num_stats (rel, value) VALUES ('%s', %s) "
                     "ON CONFLICT (rel) DO UPDATE SET value = EXCLUDED.value",
                escape((relNumData + i)->rel), (relNumData + i)->value);
        SPI_connect();
        ret = SPI_exec(command, 0);
        if (ret > 0) {
            SPI_finish();
        }
        pfree(command);
    }
}

RetroConfig* getRetroConfig(const char* json, jsmntok_t* t, int* size) {
    RetroConfig* ret = palloc(sizeof(RetroConfig));
    int index = 1;

    *size = t[0].size;
    for (int i = 0; i < t[0].size; i++) {

        if (jsoneq(json, &t[index], "WE_ORIGINAL_TABLE_NAME") == 0) {
            ret->weOriginalTableName = sprintf_json(json, &t[index + 1]);
        } else if (jsoneq(json, &t[index], "RETRO_TABLE_CONFS") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->retroTableConfs = palloc(t[index + 1].size * sizeof(char*));
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
                ret->tableBlacklist = palloc(t[index + 1].size * sizeof(char*));
                for (int j = 0; j < t[index + 1].size; j++) {
                    ret->tableBlacklist[j] = sprintf_json(json, &t[index + j + 2]);
                }
            }
            index += t[index + 1].size;
        } else if (jsoneq(json, &t[index], "COLUMN_BLACKLIST") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->columnBlacklist = palloc(t[index + 1].size * sizeof(char*));
                for (int j = 0; j < t[index + 1].size; j++) {
                    ret->columnBlacklist[j] = sprintf_json(json, &t[index + j + 2]);
                }
            }
            index += t[index + 1].size;
        } else if (jsoneq(json, &t[index], "RELATION_BLACKLIST") == 0) {
            if (t[index + 1].type == JSMN_ARRAY) {
                ret->relationBlacklist = palloc(t[index + 1].size * sizeof(char*));
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