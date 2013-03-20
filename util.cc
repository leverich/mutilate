#include <inttypes.h>
#include <stdio.h>
#include <sys/time.h>
#include <string>
#include <string.h>
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

// Split args.server_arg[s] into host:port using strtok(). Returns true upon
// success
int parse_host(const char *in, std::string& hostname, std::string& port) {
  char *saveptr = NULL;  // For reentrant strtok().
  char *s_copy = new char[strlen(in) + 1];
  strcpy(s_copy, in);

  char *h_ptr = strtok_r(s_copy, ":", &saveptr);
  char *p_ptr = strtok_r(NULL, ":", &saveptr);

  if (h_ptr == NULL) return 0;

  hostname = h_ptr;
  port = "11211";
  if (p_ptr) port = p_ptr;

  delete[] s_copy;

  return 1;
}
