#include <netinet/tcp.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "config.h"

#include "Connection.h"
#include "distributions.h"
#include "Generator.h"
#include "mutilate.h"
#include "binary_protocol.h"
#include "util.h"

Connection::Connection(struct event_base* _base, struct evdns_base* _evdns,
                       string hostname, string port, options_t _options,
                       bool sampling) :
        start_time(0), stats(sampling), options(_options),
        base(_base), evdns(_evdns), vb(NULL)
{
  valuesize = createGenerator(options.valuesize);
  keysize = createGenerator(options.keysize);
  keygen = new KeyGenerator(keysize, options.records);

  if (options.lambda <= 0) {
    iagen = createGenerator("0");
  } else {
    D("iagen = createGenerator(%s)", options.ia);
    iagen = createGenerator(options.ia);
    iagen->set_lambda(options.lambda);
  }

  read_state = INIT_READ;
  write_state = INIT_WRITE;

  struct bufferevent *bev;
  bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev, bev_read_cb, bev_write_cb, bev_event_cb, this);
  bufferevent_enable(bev, EV_READ | EV_WRITE);

  if (bufferevent_socket_connect_hostname(bev, evdns, AF_UNSPEC,
                                          hostname.c_str(),
                                          atoi(port.c_str())))
    DIE("bufferevent_socket_connect_hostname()");

  timer = evtimer_new(base, timer_cb, this);

  single_connection conn(hostname, port, bev);
  conns.push_back(conn);
}

Connection::Connection(struct event_base* _base, struct evdns_base* _evdns,
                       options_t options, VBUCKET_CONFIG_HANDLE vb,
                       bool sampling) :
  start_time(0), stats(sampling), options(options),
  base(_base), evdns(_evdns), conns(), vb(vb)
{
  valuesize = createGenerator(options.valuesize);
  keysize = createGenerator(options.keysize);
  keygen = new KeyGenerator(keysize, options.records);

  if (options.lambda <= 0) {
    iagen = createGenerator("0");
  } else {
    D("iagen = createGenerator(%s)", options.ia);
    iagen = createGenerator(options.ia);
    iagen->set_lambda(options.lambda);
  }

  read_state = INIT_READ;
  write_state = INIT_WRITE;

  for (int i = 0; i < vbucket_config_get_num_servers(vb); i ++) {
    struct bufferevent *bev;
    string hostname, port;
    const char *server = vbucket_config_get_server(vb, i);
    if(!parse_host(server, hostname, port))
      DIE("strtok(.., \":\") failed to parse %s", server);

    bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
    bufferevent_setcb(bev, bev_read_cb, bev_write_cb, bev_event_cb, this);
    bufferevent_enable(bev, EV_READ | EV_WRITE);

    if (bufferevent_socket_connect_hostname(bev, evdns, AF_UNSPEC,
                                            hostname.c_str(),
                                            atoi(port.c_str())))
      DIE("bufferevent_socket_connect_hostname()");

    timer = evtimer_new(base, timer_cb, this);


    single_connection conn(hostname, port, bev);
    conns.push_back(conn);
  }
}

Connection::~Connection() {
  event_free(timer);
  timer = NULL;

  // FIXME:  W("Drain op_q?");
  for (struct single_connection& conn : conns) {
    free(conn.bev);
  }

  delete iagen;
  delete keygen;
  delete keysize;
  delete valuesize;
}

void Connection::reset() {
  // FIXME: Actually check the connection, drain all bufferevents, drain op_q.
  assert(op_queues_size() == 0);
  evtimer_del(timer);
  read_state = IDLE;
  write_state = INIT_WRITE;
  stats = ConnectionStats(stats.sampling);
}


void Connection::issue_sasl(struct bufferevent *bev) {
  read_state = WAITING_FOR_OP;
  auto& op_queue = find_conn(bev).op_queue;
  Operation op; op.type = Operation::SASL; op_queue.push(op);

  string username = string(options.username);
  string password = string(options.password);

  binary_header_t header = {0x80, CMD_SASL, 0, 0, 0, 0, 0, 0, 0};
  header.key_len = htons(5);
  header.body_len = htonl(6 + username.length() + 1 + password.length());

  bufferevent_write(bev, &header, 24);
  bufferevent_write(bev, "PLAIN\0", 6);
  bufferevent_write(bev, username.c_str(), username.length() + 1);
  bufferevent_write(bev, password.c_str(), password.length());
}

void Connection::issue_get(const char* key, double now) {
  struct bufferevent *bev = NULL;
  std::queue<Operation>* op_queue;
  uint16_t keylen = strlen(key);
  uint16_t vbucket_id = 0;
  Operation op;
  int l;

  //TODO(syang0) Test, comment this out and you should see misses.
  if (vb) {
    vbucket_id = vbucket_get_vbucket_by_key(vb, key, keylen);
    int serverIndex = vbucket_get_master(vb, vbucket_id);
    single_connection& conn = conns[serverIndex];
    op_queue = &conn.op_queue;
    bev = conn.bev;
  } else {
    single_connection& conn = conns.front();
    op_queue = &(conn.op_queue);
    bev = conn.bev;
  }


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

  op_queue->push(op);

  if (read_state == IDLE)
    read_state = WAITING_FOR_OP;

  if (options.binary) {
    // each line is 4-bytes
    uint16_t vbucket_id = vb ? vbucket_get_vbucket_by_key(vb, key, keylen) : 0;
    binary_header_t h = {0x80, CMD_GET, htons(keylen),
                       0x00, 0x00, htons(vbucket_id),
                       htonl(keylen) };

    bufferevent_write(bev, &h, 24); // size does not include extras
    bufferevent_write(bev, key, keylen);
    l = 24 + keylen;
  } else {
    l = evbuffer_add_printf(bufferevent_get_output(bev), "get %s\r\n", key);
  }

  if (read_state != LOADING) stats.tx_bytes += l;
}

void Connection::issue_set(const char* key, const char* value, int length,
                           double now) {
  struct bufferevent *bev = NULL;
  std::queue<Operation>* op_queue;
  uint16_t keylen = strlen(key);
  uint16_t vbucket_id = 0;
  Operation op;
  int l;

  if (vb) {
    vbucket_id = vbucket_get_vbucket_by_key(vb, key, keylen);
    int serverIndex = vbucket_get_master(vb, vbucket_id);
    single_connection& conn = conns[serverIndex];
    op_queue = &conn.op_queue;
    bev = conn.bev;
  } else {
    single_connection& conn = conns.front();
    op_queue = &(conn.op_queue);
    bev = conn.bev;
  }

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) op.start_time = get_time();
  else op.start_time = now;
#endif

  op.type = Operation::SET;
  op_queue->push(op);

  if (read_state == IDLE)
    read_state = WAITING_FOR_OP;

  if (options.binary) {
    // each line is 4-bytes
    binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, htons(vbucket_id),
                        htonl(keylen + 8 + length)};

    bufferevent_write(bev, &h, 32); // With extras
    bufferevent_write(bev, key, keylen);
    bufferevent_write(bev, value, length);
    l = 24 + h.body_len;
  } else {
    l = evbuffer_add_printf(bufferevent_get_output(bev),
                                "set %s 0 0 %d\r\n", key, length);
    bufferevent_write(bev, value, length);
    bufferevent_write(bev, "\r\n", 2);
    l += length + 2;
  }

  if (read_state != LOADING) stats.tx_bytes += l;
}

void Connection::issue_something(double now) {
  char key[256];
  // FIXME: generate key distribution here!
  string keystr = keygen->generate(lrand48() % options.records);
  strcpy(key, keystr.c_str());
  //  int key_index = lrand48() % options.records;
  //  generate_key(key_index, options.keysize, key);

  if (drand48() < options.update) {
    int index = lrand48() % (1024 * 1024);
    //    issue_set(key, &random_char[index], options.valuesize, now);
    issue_set(key, &random_char[index], valuesize->generate(), now);
  } else {
    issue_get(key, now);
  }
}

void Connection::pop_op(std::queue<Operation>& op_queue) {
  assert(op_queue.size() > 0);

  op_queue.pop();

  if (read_state == LOADING) return;
  read_state = IDLE;

  // Advance the read state machine.
  if (op_queue.size() > 0) {
    Operation& op = op_queue.front();
    switch (op.type) {
      case Operation::GET:
      case Operation::SET:
        read_state = WAITING_FOR_OP;
        break;
    default: DIE("Not implemented.");
    }
  }
}

bool Connection::check_exit_condition(double now) {
  if (read_state == INIT_READ) return false;
  if (now == 0.0) now = get_time();
  if (now > start_time + options.time) return true;
  if (options.loadonly && read_state == IDLE) return true;
  return false;
}

// drive_write_machine() determines whether or not to issue a new
// command.  Note that this function loops.  Be wary of break
// vs. return.

void Connection::drive_write_machine(double now) {
  if (now == 0.0) now = get_time();

  double delay;
  struct timeval tv;

  if (check_exit_condition(now)) return;

  while (1) {
    switch (write_state) {
    case INIT_WRITE:
      /*
      if (options.iadist == EXPONENTIAL)
        delay = generate_poisson(options.lambda);
      else
        delay = generate_uniform(options.lambda);
      */
      delay = iagen->generate();

      next_time = now + delay;
      double_to_tv(delay, &tv);
      evtimer_add(timer, &tv);

      write_state = WAITING_FOR_TIME;
      break;

    case ISSUING:
      if (op_queues_size() >= (size_t) options.depth) {
        write_state = WAITING_FOR_OPQ;
        return;
      } else if (now < next_time) {
        write_state = WAITING_FOR_TIME;
        break; // We want to run through the state machine one more time
               // to make sure the timer is armed.
      }

      issue_something(now);
      stats.log_op(op_queues_size());

      /*
      if (options.iadist == EXPONENTIAL)
        next_time += generate_poisson(options.lambda);
      else
        next_time += generate_uniform(options.lambda);
      */
      next_time += iagen->generate();

      break;

    case WAITING_FOR_TIME:
      if (now < next_time) {
        if (!event_pending(timer, EV_TIMEOUT, NULL)) {
          delay = next_time - now;
          double_to_tv(delay, &tv);
          evtimer_add(timer, &tv);
        }
        return;
      }

      write_state = ISSUING;
      break;

    case WAITING_FOR_OPQ:
      if (op_queues_size() >= (size_t) options.depth) return;
      write_state = ISSUING;
      break;

    default: DIE("Not implemented");
    }
  }
}

void Connection::event_callback(struct bufferevent *bev, short events) {
  //  struct timeval now_tv;
  // event_base_gettimeofday_cached(base, &now_tv);

  if (events & BEV_EVENT_CONNECTED) {
    single_connection conn = find_conn(bev);
    D("Connected to %s:%s.", conn.hostname.c_str(), conn.port.c_str());
    int fd = bufferevent_getfd(bev);
    if (fd < 0) DIE("bufferevent_getfd");

    if (!options.no_nodelay) {
      int one = 1;
      if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     (void *) &one, sizeof(one)) < 0)
        DIE("setsockopt()");
    }

    if (options.sasl)
      issue_sasl(bev);
    else
      read_state = IDLE;  // This is the most important part!
  } else if (events & BEV_EVENT_ERROR) {
    int err = bufferevent_socket_get_dns_error(bev);
    if (err) DIE("DNS error: %s", evutil_gai_strerror(err));

    DIE("BEV_EVENT_ERROR: %s", strerror(errno));
  } else if (events & BEV_EVENT_EOF) {
    DIE("Unexpected EOF from server.");
  }
}

void Connection::read_callback(struct bufferevent *bev) {
  auto& op_queue = find_conn(bev).op_queue;
  struct evbuffer *input = bufferevent_get_input(bev);
#if USE_CACHED_TIME
  struct timeval now_tv;
  event_base_gettimeofday_cached(base, &now_tv);
#endif

  char *buf;
  Operation *op = NULL;
  bool finished;

  double now;

  // Protocol processing loop.

  if (op_queue.size() == 0) V("Spurious read callback.");

  while (1) {
    if (op_queue.size() > 0) op = &op_queue.front();

    switch (read_state) {
    case INIT_READ: DIE("event from uninitialized connection");
    case IDLE: return;  // We munched all the data we expected?

    // Note: for binary, the whole get suite (GET, GET_DATA, END) is collapsed
    // into one state
    case WAITING_FOR_OP:
      assert(op_queue.size() > 0);

      finished = (options.binary) ?  consume_binary_response(input) :
                                          consume_ascii_response(input, op);
      if (!finished)
        return;

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
      if (op->type == Operation::GET)
        stats.log_get(*op);

      if (op->type == Operation::SET)
        stats.log_get(*op);

      pop_op(op_queue);
      drive_write_machine(now);
      break;

    case LOADING:
      assert(op_queue.size() > 0);

      if (options.binary) {
        if (!consume_binary_response(input)) return;
      } else {
        buf = evbuffer_readln(input, NULL, EVBUFFER_EOL_CRLF);
        if (buf == NULL) return; // Haven't received a whole line yet.
        free(buf);
      }

      loader_completed++;
      pop_op(op_queue);

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

/**
 * Tries to consume a binary response (in its entirety) from an evbuffer.
 *
 * @param input evBuffer to read response from
 * @return  true if consumed, false if not enough data in buffer.
 */
bool Connection::consume_binary_response(evbuffer *input) {
  // Read the first 24 bytes as a header
  int length = evbuffer_get_length(input);
  if (length < 24) return false;
  binary_header_t* h =
          reinterpret_cast<binary_header_t*>(evbuffer_pullup(input, 24));
  assert(h);

  // Not whole response
  int targetLen = 24 + ntohl(h->body_len);
  if (length < targetLen) {
    return false;
  }

  // if something other than success, count it as a miss
  if (h->opcode == CMD_GET && h->status != RESP_OK) {
      stats.get_misses++;
  }

  #define unlikely(x)     __builtin_expect((x),0)
  if (unlikely(h->opcode == CMD_SASL)) {
    if (h->status == RESP_OK) {
      V("SASL authentication succeeded");
    } else {
      DIE("SASL authentication failed");
    }
  }

  evbuffer_drain(input, targetLen);
  stats.rx_bytes += targetLen;
  return true;
}

bool Connection::consume_ascii_response(evbuffer *input, Operation *op) {
  struct evbuffer_ptr ptr;
  size_t eol_len = 0;
  int ret = 0;

  // Wait for first line
  ptr = evbuffer_search_eol(input, NULL, &eol_len, EVBUFFER_EOL_CRLF_STRICT);
  if (ptr.pos == -1) return false; // Don't have first line yet.

  switch (op->type) {
    case Operation::GET:
      // is it "END"?
      if (ptr.pos == 3) {
        stats.get_misses++;
        evbuffer_drain(input, ptr.pos + eol_len);
        stats.rx_bytes += ptr.pos + eol_len;
        return true;
      }

      // look for 2x /r/n.
      ret |= evbuffer_ptr_set(input, &ptr, eol_len, EVBUFFER_PTR_ADD);
      ptr = evbuffer_search_eol(input, &ptr, &eol_len, EVBUFFER_EOL_CRLF_STRICT);
      if (ptr.pos == -1 || ret) return false; // haven't gotten another line

      ret |= evbuffer_ptr_set(input, &ptr, eol_len, EVBUFFER_PTR_ADD);
      ptr = evbuffer_search_eol(input, &ptr, &eol_len, EVBUFFER_EOL_CRLF_STRICT);
      if (ptr.pos == -1 || ret) return false; // haven't gotten another line

      // Fall through to drain.
    case Operation::SET:
      evbuffer_drain(input, ptr.pos + eol_len);
      stats.rx_bytes += ptr.pos + eol_len;
      return true;

    default:
      DIE("Internal Error: invalid op->type in consume_ascii_response. %d",
              op->type);
  }
  return false;
}

void Connection::write_callback() {}
void Connection::timer_callback() { drive_write_machine(); }

// The follow are C trampolines for libevent callbacks.
void bev_event_cb(struct bufferevent *bev, short events, void *ptr) {
  Connection* conn = (Connection*) ptr;
  conn->event_callback(bev, events);
}

void bev_read_cb(struct bufferevent *bev, void *ptr) {
  Connection* conn = (Connection*) ptr;
  conn->read_callback(bev);
}

void bev_write_cb(struct bufferevent *bev, void *ptr) {
  Connection* conn = (Connection*) ptr;
  conn->write_callback();
}

void timer_cb(evutil_socket_t fd, short what, void *ptr) {
  Connection* conn = (Connection*) ptr;
  conn->timer_callback();
}

void Connection::set_priority(int pri) {
  for (single_connection conn : conns) {
    if (bufferevent_priority_set(conn.bev, pri))
      DIE("bufferevent_set_priority(bev, %d) failed", pri);
  }
}

void Connection::start_loading() {
  read_state = LOADING;
  loader_issued = loader_completed = 0;

  for (int i = 0; i < LOADER_CHUNK; i++) {
    if (loader_issued >= options.records) break;

    char key[256];
    int index = lrand48() % (1024 * 1024);
    string keystr = keygen->generate(loader_issued);
    strcpy(key, keystr.c_str());
          //    generate_key(loader_issued, options.keysize, key);
    //    issue_set(key, &random_char[index], options.valuesize);
    issue_set(key, &random_char[index], valuesize->generate());
    loader_issued++;
  }
}

Connection::single_connection& Connection::find_conn(struct bufferevent *bev) {
  // Fast path for standard mutilate.
  if (conns.size() == 0)
    return conns.front();

  for(single_connection& conn: conns) {
    if (conn.bev == bev)
      return conn;
  }

  DIE("Can't find op_queue with associated *bev");
}

bool Connection::isIdle() {
  return read_state == IDLE;
}

int Connection::op_queues_size() {
  int cnt = 0;
  for (single_connection& conn: conns)
    cnt += conn.op_queue.size();
  return cnt;
}
