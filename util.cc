#include <inttypes.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

#include "mutilate.h"
#include "util.h"

void sleep_time(double duration) {
  if (duration > 0) usleep((useconds_t) (duration * 1000000));
}

#define FNV_64_PRIME (0x100000001b3ULL)
#define FNV1_64_INIT (0xcbf29ce484222325ULL)
uint64_t fnv_64_buf(const void* buf, size_t len) {
  uint64_t hval = FNV1_64_INIT;

  unsigned char *bp = (unsigned char *)buf;   /* start of buffer */
  unsigned char *be = bp + len;               /* beyond end of buffer */

  while (bp < be) {
    hval ^= (uint64_t)*bp++;
    hval *= FNV_64_PRIME;
  }

  return hval;
}

void generate_key(int n, int length, char *buf) {
  snprintf(buf, length + 1, "%0*d", length, n);
}
