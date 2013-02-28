#include <netinet/tcp.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "config.h"

#include "ConnectionBinary.h"
#include "distributions.h"
#include "Generator.h"
#include "mutilate.h"
#include "util.h"

void ConnectionBinary::issue_get(const char* key, double now) {
  Operation op;

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base, &now_tv);

    op.start_time = tv_to_double(&now_tv);
#else
    op.start_time = get_time();
#endif
  } else {
    op.start_time = now;
  }
#endif

  op.type = Operation::GET;
  op.key = string(key);

  op_queue.push(op);

  if (read_state == IDLE)
    read_state = WAITING_FOR_GET;

  uint32_t kl = strlen(key);
  binary_header_t header = {0x80, 0, 0, 0, 0, 0, 0, 0, 0};
  header.key_len = htons(kl);
  header.body_len = htonl(kl);

  bufferevent_write(bev, &header, 24);
  bufferevent_write(bev, key, kl);

  if (read_state != LOADING) stats.tx_bytes += 24 + kl;
}

void ConnectionBinary::issue_set(const char* key, const char* value,
                                 int length, double now) {
  Operation op;

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) op.start_time = get_time();
  else op.start_time = now;
#endif

  op.type = Operation::SET;
  op_queue.push(op);

  if (read_state == IDLE)
    read_state = WAITING_FOR_SET;

  uint32_t kl = strlen(key);
  uint32_t bl = kl + 8 + (uint32_t) length;
  binary_header_t header = {0x80, 1, 0, 8, 0, 0, 0, 0, 0};
  header.key_len = htons(kl);
  header.body_len = htonl(bl);
  char extras[] = {0, 0, 0, 0, 0, 0, 0, 0};

  bufferevent_write(bev, &header, 24);
  bufferevent_write(bev, extras, 8); // FIXME: can collapse this and above call
  bufferevent_write(bev, key, kl);
  bufferevent_write(bev, value, length);

  if (read_state != LOADING) stats.tx_bytes += 32 + kl + length;
}

void ConnectionBinary::read_callback() {
  struct evbuffer *input = bufferevent_get_input(bev);
#if USE_CACHED_TIME
  struct timeval now_tv;
  event_base_gettimeofday_cached(base, &now_tv);
#endif

  binary_header_t header;
  char *buf;
  Operation *op = NULL;
  int length;
  size_t n_read_out;

  double now;

  // Protocol processing loop.

  if (op_queue.size() == 0) V("Spurious read callback.");

  while (1) {
    if (op_queue.size() > 0) op = &op_queue.front();

    switch (read_state) {
    case INIT_READ: DIE("event from uninitialized connection");
    case IDLE: return;  // We munched all the data we expected?

    case WAITING_FOR_GET:
      assert(op_queue.size() > 0);

      // check we have at least the header
      length = evbuffer_get_length(input);
      if (length < 24) return;

      // get header and see if we have complete response
      evbuffer_remove(input, &header, 24);
      header.body_len = ntohl(header.body_len);
      header.status = ntohs(header.status);

      stats.rx_bytes += 24; // TODO: Should we count this? Is it really part of goodput?

      if (header.status == 1) {
        //        D("GET (%s) miss.", op->key.c_str());
        stats.get_misses++;

#if USE_CACHED_TIME
        now = tv_to_double(&now_tv);
#else
        now = get_time();
#endif
#if HAVE_CLOCK_GETTIME
        op->end_time = get_time_accurate();
#else
        op->end_time = now;
#endif

        stats.log_get(*op);

        pop_op();
        drive_write_machine();
        break;
      } else if (header.status == 0) {
        // FIXME: check key name to see if it corresponds to the op at
        // the head of the op queue?  This will be necessary to
        // support "gets" where there may be misses.

        data_length = header.body_len;
        read_state = WAITING_FOR_GET_DATA;
      }

    case WAITING_FOR_GET_DATA:
      assert(op_queue.size() > 0);

      length = evbuffer_get_length(input);
      if (length < data_length) return;

      // FIXME: Actually parse the value?  Right now we just drain it.
      evbuffer_drain(input, data_length);

      stats.rx_bytes += data_length;

#if USE_CACHED_TIME
      now = tv_to_double(&now_tv);
#else
      now = get_time();
#endif
#if HAVE_CLOCK_GETTIME
      op->end_time = get_time_accurate();
#else
      op->end_time = now;
#endif

      stats.log_get(*op);

      pop_op();
      drive_write_machine(now);
      break;

    case WAITING_FOR_SET:
      assert(op_queue.size() > 0);

      // check we have at least the header
      length = evbuffer_get_length(input);
      if (length < 24) return;

      // get header and see if we have complete response
      evbuffer_copyout(input, &header, 24);
      header.body_len = ntohl(header.body_len);
      if (length < (24 + header.body_len)) return;

      // we do, so drain whole response
      evbuffer_drain(input, 24 + header.body_len);

      stats.rx_bytes += n_read_out;

      now = get_time();

#if HAVE_CLOCK_GETTIME
      op->end_time = get_time_accurate();
#else
      op->end_time = now;
#endif

      stats.log_set(*op);

      pop_op();
      drive_write_machine(now);
      break;

    case LOADING:
      assert(op_queue.size() > 0);

      // check we have at least the header
      length = evbuffer_get_length(input);
      if (length < 24) return;

      // get header and see if we have complete response
      evbuffer_copyout(input, &header, 24);
      header.body_len = ntohl(header.body_len);
      if (length < (24 + header.body_len)) return;

      // we do, so drain whole response
      evbuffer_drain(input, 24 + header.body_len);

      loader_completed++;
      pop_op();

      if (loader_completed == options.records) {
        D("Finished loading.");
        read_state = IDLE;
      } else {
        while (loader_issued < loader_completed + LOADER_CHUNK) {
          if (loader_issued >= options.records) break;

          char key[256];
          string keystr = keygen->generate(loader_issued);
          strcpy(key, keystr.c_str());
          int index = lrand48() % (1024 * 1024);
          //          generate_key(loader_issued, options.keysize, key);
          //          issue_set(key, &random_char[index], options.valuesize);
          issue_set(key, &random_char[index], valuesize->generate());

          loader_issued++;
        }
      }

      break;

    case SASL:
      // check we have at least the header
      length = evbuffer_get_length(input);
      if (length < 24) return;

      // get header and see if we have complete response
      evbuffer_copyout(input, &header, 24);
      header.body_len = ntohl(header.body_len);
      if (length < (24 + header.body_len)) return;

      // we do, so drain whole response
      evbuffer_drain(input, 24 + header.body_len);

      // check SASL response was OK.
      if (header.status != 0) DIE("SASL authentication failed");
      D("SASL authentication succeeded");
      read_state = IDLE;
      break;

    default: DIE("not implemented");
    }
  }
}

