#ifndef SIMILARITY_H
#define SIMILARITY_H

// clang-format off

#include "postgres.h"

// clang-format on

double cosine_similarity_simple(Datum* v1, Datum* v2, int size);

double cosine_similarity_simple_norm(Datum* v1, Datum* v2, int size);

int cosine_similarty_complex(int** vectors);

#endif /*SIMILARITY_H*/
