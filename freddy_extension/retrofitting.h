#ifndef RETROFITTING_H
#define RETROFITTING_H

// clang-format off

#include "postgres.h"

#define JSMN_HEADER
#include "jsmn.h"


// clang-format on

typedef struct ColumnData {
    char* name;
    char* mean;
    int size;
} ColumnData;

typedef struct CardinalityData {
    char* value;
    int cardinality;
} CardinalityData;

typedef struct RelationColumnData {
    char* name;
    char* centroid;
    int size;
    CardinalityData* cardinalities;
} RelationColumnData;

typedef struct RelationData {
    char* relation_name;
    char* name;
    RelationColumnData* col1;
    RelationColumnData* col2;
    int max_r;
    int max_c;
} RelationData;

typedef struct RelNumData {
    char* rel;
    int value;
} RelNumData;

typedef struct DeltaElem {
    const char* name;
    const char* value;
} DeltaElem;

typedef struct DeltaCat {
    const char* name;
    const char* type;
    DeltaElem* elements;
    const char* query;
    DeltaElem* inferredElements;
} DeltaCat;

typedef struct DeltaRel {
    const char* relation;
    const char* name;
    const char* type;
    DeltaElem* elements;
} DeltaRel;

typedef struct RetroConfig {
    const char* weOriginalTableName;
    const char** retroTableConfs;
    const char* schemaGraphPath;
    const char* schemaJsonGraphPath;
    const char** tableBlacklist;
    const char** columnBlacklist;
    const char** relationBlacklist;
    const char* outputFolder;
    const char* groupsFileName;
    int iterations;
    const char* retroVecsFileName;
    const char* tokenization;
    int alpha;
    int beta;
    int gamma;
    int delta;
} RetroConfig;

int jsoneq(const char *json, jsmntok_t *tok, const char *s);

jsmntok_t* readJsonFile(const char* path, char** json, int* r);

char* sprintf_json(const char* json, jsmntok_t* t);

char* escape(char* str);

ColumnData* getColumnData(const char* json, jsmntok_t* t, int k, int* columnDataSize);

CardinalityData* getCardinalities(const char* json, jsmntok_t* t, int k, int size, int* count);

RelationColumnData* getRelationColumnData(const char* json, jsmntok_t* t, int k, int* relationDataSize);

RelationData* getRelationData(const char* json, jsmntok_t* t, int k, int* relationDataSize);

RelNumData* getRelNumData(const char* json, jsmntok_t* t, int k, int* relNumDataSize);

DeltaElem* getDeltaElems(const char* json, jsmntok_t* t, int k, int* elemSize);

DeltaCat* getDeltaCats(const char* json, jsmntok_t* t, int maxTok, int* count, int* lastIndex);

DeltaRel* getDeltaRels(const char* json, jsmntok_t* t, int maxTok, int* count, int start);

void clearStats(void);

void updateColumnStatistics(ColumnData* columnData, int columnCount);

void updateCardinalityStatistics(struct CardinalityData* cardinalityData, int count, char* relColId);

char* updateRelColStatistics(struct RelationColumnData* relationColumnData);

void updateRelationStatistics(struct RelationData* relationData, int count);

void updateRelNumDataStatistics(RelNumData* relNumData, int relCount);

RetroConfig* getRetroConfig(const char* json, jsmntok_t* t, int* count);

#endif //RETROFITTING_H
