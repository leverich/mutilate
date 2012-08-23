#ifndef CONNECTIONOPTIONS_H
#define CONNECTIONOPTIONS_H

#include "distributions.h"

typedef struct {
  int connections;
  bool blocking;
  double lambda;
  int qps;
  int records;

  char keysize[32];
  char valuesize[32];
  // int keysize;
  //  int valuesize;
  char ia[32];

  // qps_per_connection
  // iadist

  double update;
  int time;
  bool loadonly;
  int depth;
  bool no_nodelay;
  bool noload;
  int threads;
  enum distribution_t iadist;
  int warmup;

  bool roundrobin;
  int server_given;
  int lambda_denom;

  bool oob_thread;
} options_t;

#endif // CONNECTIONOPTIONS_H
