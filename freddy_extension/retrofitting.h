#ifndef RETROFITTING_H
#define RETROFITTING_H

// clang-format off

#include "postgres.h"
#include "hashmap.h"

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
    char* name;
    char* value;
} DeltaElem;

typedef struct DeltaCat {
    char* name;
    char* type;
    int elementCount;
    DeltaElem* elements;
    char* query;
    int inferredElementCount;
    DeltaElem* inferredElements;
} DeltaCat;

typedef struct DeltaRelElem {
    char* name;
    char* type;
    int elementCount;
    DeltaElem* elements;
} DeltaRelElem;

typedef struct DeltaRel {
    char* relation;
    DeltaRelElem* groups;
    int groupCount;
} DeltaRel;

typedef struct ProcessedRel {
    char* key;
    char** terms;
    int termCount;
    char* target;
} ProcessedRel;

typedef struct ProcessedDeltaEntry {
    char* name;
    char* vector;
    char* column;
    int relationCount;
    ProcessedRel* relations;
} ProcessedDeltaEntry;

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

typedef struct RetroVec {
    int id;
    char* word;
    float* vector;
    int dim;
} RetroVec;

typedef struct WordVec {
    int id;
    char* word;
    float* vector;
    int dim;
} WordVec;

typedef struct RadixTree {
    char* value;
    float* vector;
    int id;
    struct hashmap* children;
} RadixTree;

typedef struct retroQuery {
    char* command;
    char* cur;
} retroQuery;

typedef struct l2IterStr {
    struct hashmap* vecs;
    int dim;
    double delta;
} l2IterStr;

typedef struct dbElem {
    char* key;
    int value;
} dbElem;

typedef struct retroPointer {
    struct hashmap* max_r_map;
    struct hashmap* max_c_map;
    struct hashmap* size_map;
    struct hashmap* value_map;
    struct hashmap* cardinality_map;
} retroPointer;

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

void insertColumnStatistics(ColumnData* columnData, int columnCount);

void updateCardinalityStatistics(struct CardinalityData* cardinalityData, int count, char* relColId);

char* updateRelColStatistics(struct RelationColumnData* relationColumnData);

void insertRelationStatistics(struct RelationData* relationData, int count);

void insertRelNumDataStatistics(RelNumData* relNumData, int relCount);

RetroConfig* getRetroConfig(const char* json, jsmntok_t* t, int* count);

int wordVecCompare(const void *a, const void *b, void *udata);

uint64_t wordVecHash(const void *item, uint64_t seed0, uint64_t seed1);

struct hashmap* getWordVecs(char* tableName, int* dim);

WordVec* getWordVec(char* tableName, char* word);

int getIntFromDB(char* query);

char* getJoinRelFromDB(char* table, char* foreign_table);

bool getRetroQuerySize(const void* item, void* s);

bool buildRetroQuery(const void *item, void *q);

void deleteRetroVecsDB(const char* tableName);

void retroVecsToDB(const char* tableName, struct hashmap* retroVecs, int dim);

float* calcColMean(char* tableName, char* column, char* vecTable, RadixTree* vecTree, const char* tokenization, int dim);

void insertColMeanToDB(char* tabCol, float* mean, const char* tokenization, int dim);

float* getColMeanFromDB(char* tabCol, const char* tokenization, int dim);

float* getColMean(char* column, char* vecTable, RadixTree* vecTree, const char* tokenization, int dim);

float* calcCentroid(ProcessedRel* relation, char* retroVecTable, int dim);

void updateCentroidInDB(ProcessedRel* relation, float* centroid, int dim);

float* getCentroidFromDB(ProcessedRel* relation);

float* getCentroid(ProcessedRel* relation, char* retroVecTable, int dim);

void* prealloc(void* ptr, size_t new_s);

void processDeltaElements(ProcessedDeltaEntry* result, DeltaCat* deltaCat, int elementCount, DeltaElem* elements, int* index);

void addPrecessRel(ProcessedDeltaEntry* elem, DeltaRelElem* deltaRelelem, char* relation, char* term, char* target);

ProcessedDeltaEntry* processDelta(DeltaCat* deltaCat, int deltaCatCount, DeltaRel* deltaRel, int deltaRelCount, int* count);

void addMissingVecs(struct hashmap* retroVecs, ProcessedDeltaEntry* processedDelta, int processedDeltaCount, RadixTree* vecTree, const char* tokenizationStrategy, int dim);

int radixCompare(const void *a, const void *b, void *udata);

uint64_t radixHash(const void *item, uint64_t seed0, uint64_t seed1);

bool addEntryToRadixTree(const void* item, void* ref);

RadixTree* buildRadixTree(struct hashmap* vecs, int dim);

void inferAdd(float* result, RadixTree* tree, int* tokens, const char* tokenizationStrategy, int dim);

float* inferVec(char* term, RadixTree* tree, char* delimiter, const char* tokenizationStrategy, int dim);

void sumVecs(float* f1, float* f2, int dim);

void add2Vec(float* vec1, float add, int dim);

void subFromVec(float* vec1, float sub, int dim);

void subVecs(float* vec1, float* vec2, int dim);

void multVec(float* vec, int mult, int dim);

void multVecF(float* vec, float mult, int dim);

void divVec(float* f1, int divisor, int dim);

void divVecF(float* vec, float divisor, int dim);

void clearVec(float* f1, int dim);

int isZeroVec(float* vec, int dim);

void printVec(float* vec, int dim);

char** getNToks(char* word, char* delim, int size);

char** getTableAndCol(char* word);

char** getTableAndColFromRel(char* word);

void resizeQuery(char* query, int* currentSize, int maxNeeded);

int getInt(struct hashmap* map, char* query);

int dbElemCompare(const void *a, const void *b, void *data);

uint64_t dbElemHash(const void *item, uint64_t seed0, uint64_t seed1);

struct hashmap* calcRetroVecs(ProcessedDeltaEntry* processedDelta, int processedDeltaCount, struct hashmap* retroVecs, RetroConfig* retroConfig, RadixTree* vecTree, retroPointer* pointer, int dim);

bool iterUpdate(const void* item, void* data);

void updateRetroVecs(struct hashmap* retroVecs, struct hashmap* newVecs);

bool iterL2(const void* item, void* data);

double calcDelta(struct hashmap* retroVecs, struct hashmap* newVecs, int dim);

float calcL2Norm(float* vec1, float* vec2, int dim);

#endif //RETROFITTING_H
