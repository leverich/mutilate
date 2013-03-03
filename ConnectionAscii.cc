#include <netinet/tcp.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "config.h"

#include "ConnectionAscii.h"
#include "distributions.h"
#include "Generator.h"
#include "mutilate.h"
#include "util.h"

void ConnectionAscii::issue_get(const char* key, double now) {
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

  int l = evbuffer_add_printf(bufferevent_get_output(bev), "get %s\r\n", key);
  if (read_state != LOADING) stats.tx_bytes += l;
}

void ConnectionAscii::issue_set(const char* key, const char* value, int length,
                           double now) {
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

  int l = evbuffer_add_printf(bufferevent_get_output(bev),
                              "set %s 0 0 %d\r\n", key, length);
  bufferevent_write(bev, value, length);
  bufferevent_write(bev, "\r\n", 2);

  if (read_state != LOADING) stats.tx_bytes += l + length + 2;
}

void ConnectionAscii::read_callback() {
  struct evbuffer *input = bufferevent_get_input(bev);
#if USE_CACHED_TIME
  struct timeval now_tv;
  event_base_gettimeofday_cached(base, &now_tv);
#endif

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

      buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
      if (buf == NULL) return;  // A whole line not received yet. Punt.

      stats.rx_bytes += n_read_out; // strlen(buf);

      if (!strcmp(buf, "END")) {
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

        free(buf);

        pop_op();
        drive_write_machine();
        break;
      } else if (!strncmp(buf, "VALUE", 5)) {
        sscanf(buf, "VALUE %*s %*d %d", &length);

        // FIXME: check key name to see if it corresponds to the op at
        // the head of the op queue?  This will be necessary to
        // support "gets" where there may be misses.

        data_length = length;
        read_state = WAITING_FOR_GET_DATA;
      }

      free(buf);

    case WAITING_FOR_GET_DATA:
      assert(op_queue.size() > 0);

      length = evbuffer_get_length(input);

      if (length >= data_length + 2) {
        // FIXME: Actually parse the value?  Right now we just drain it.
        evbuffer_drain(input, data_length + 2);
        read_state = WAITING_FOR_END;

        stats.rx_bytes += data_length + 2;
      } else {
        return;
      }
    case WAITING_FOR_END:
      assert(op_queue.size() > 0);

      buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
      if (buf == NULL) return; // Haven't received a whole line yet. Punt.

      stats.rx_bytes += n_read_out;

      if (!strcmp(buf, "END")) {
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

        free(buf);

        pop_op();
        drive_write_machine(now);
        break;
      } else {
        DIE("Unexpected result when waiting for END");
      }

    case WAITING_FOR_SET:
      assert(op_queue.size() > 0);

      buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
      if (buf == NULL) return; // Haven't received a whole line yet. Punt.

      stats.rx_bytes += n_read_out;

      now = get_time();

#if HAVE_CLOCK_GETTIME
      op->end_time = get_time_accurate();
#else
      op->end_time = now;
#endif

      stats.log_set(*op);

      free(buf);

      pop_op();
      drive_write_machine(now);
      break;

    case LOADING:
      assert(op_queue.size() > 0);

      buf = evbuffer_readln(input, NULL, EVBUFFER_EOL_CRLF);
      if (buf == NULL) return; // Haven't received a whole line yet.
      free(buf);

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

    default: DIE("not implemented");
    }
  }
}

