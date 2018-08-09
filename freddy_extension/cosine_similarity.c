// clang-format off

#include "cosine_similarity.h"

#include "postgres.h"
#include "fmgr.h"

#include "math.h"

// clang-format on

double cosine_similarity_simple(Datum* v1, Datum* v2, int size) {
  double scalar = 0;
  double sqDistanceV1 = 0;
  double sqDistanceV2 = 0;
  int count = 0;
  for (int i = 0; i < size; i++) {
    scalar += ((double)DatumGetFloat4(v1[i])) * ((double)DatumGetFloat4(v2[i]));
    // elog(INFO, "v1 %lf, v2 %lf, vmult %lf, scalar %lf",
    // ((double)DatumGetFloat4(v1[i])), ((double)DatumGetFloat4(v2[i])),
    // ((double)DatumGetFloat4(v1[i])) * ((double)DatumGetFloat4(v2[i])),
    // scalar);
    sqDistanceV2 +=
        ((double)DatumGetFloat4(v2[i])) * ((double)DatumGetFloat4(v2[i]));
    sqDistanceV1 +=
        ((double)DatumGetFloat4(v1[i])) * ((double)DatumGetFloat4(v1[i]));
    count++;
  }
  if ((sqDistanceV1 > 0) && (sqDistanceV2 > 0)) {
    double sim = (scalar / (sqrt(sqDistanceV1) * sqrt(sqDistanceV2)));
    // elog(INFO, "scalar %lf, sqD1 %lf, sqD2 %lf sim %lf, count %d", scalar,
    // sqDistanceV1, sqDistanceV2, sim, count);
    return sim;
  } else {
    return 0;
  }
}

double cosine_similarity_simple_norm(Datum* v1, Datum* v2, int size) {
  double scalar = 0;
  for (int i = 0; i < size; i++) {
    scalar += ((double)DatumGetFloat4(v1[i])) * ((double)DatumGetFloat4(v2[i]));
  }
  return scalar;
}

int cosine_similarty_complex(int** vectors) {
  elog(WARNING, "cosine_similarty_complex is not implemented yet!");
  return 0;
}
