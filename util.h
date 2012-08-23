#ifndef UTIL_H
#define UTIL_H

#include <sys/time.h>
#include <time.h>

inline double tv_to_double(struct timeval *tv) {
  return tv->tv_sec + (double) tv->tv_usec / 1000000;
}

inline void double_to_tv(double val, struct timeval *tv) {
  long long secs = (long long) val;
  long long usecs = (long long) ((val - secs) * 1000000);

  tv->tv_sec = secs;
  tv->tv_usec = usecs;
}

inline double get_time_accurate() {
#if USE_CLOCK_GETTIME
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  //  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec + (double) ts.tv_nsec / 1000000000;
#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv_to_double(&tv);
#endif
}

inline double get_time() {
  //#if USE_CLOCK_GETTIME
  //  struct timespec ts;
  //  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  //  //  clock_gettime(CLOCK_REALTIME, &ts);
  //  return ts.tv_sec + (double) ts.tv_nsec / 1000000000;
  //#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv_to_double(&tv);
  //#endif
}

void sleep_time(double duration);

uint64_t fnv_64_buf(const void* buf, size_t len);
inline uint64_t fnv_64(uint64_t in) { return fnv_64_buf(&in, sizeof(in)); }

void generate_key(int n, int length, char *buf);

#endif // UTIL_H
