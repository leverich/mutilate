#include <netinet/tcp.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "config.h"

#include "Connection.h"
#include "ConnectionBinary.h"
#include "distributions.h"
#include "Generator.h"
#include "mutilate.h"
#include "util.h"

Connection::Connection(struct event_base* _base, struct evdns_base* _evdns,
                       string _hostname, string _port, options_t _options,
                       bool sampling) :
  hostname(_hostname), port(_port), username(""), password(""),
  start_time(0), stats(sampling), options(_options), base(_base), evdns(_evdns)
{
  username_given = false;
  startup();
}

Connection::Connection(struct event_base* _base, struct evdns_base* _evdns,
                       string _hostname, string _port, string _username,
                       string _password, options_t _options,
                       bool sampling) :
  hostname(_hostname), port(_port), username(_username), password(_password),
  start_time(0), stats(sampling), options(_options), base(_base), evdns(_evdns)
{
  username_given = true;
  startup();
}

void Connection::startup() {
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

  bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev, bev_read_cb, bev_write_cb, bev_event_cb, this);
  bufferevent_enable(bev, EV_READ | EV_WRITE);

  if (bufferevent_socket_connect_hostname(bev, evdns, AF_UNSPEC,
                                          hostname.c_str(),
                                          atoi(port.c_str())))
    DIE("bufferevent_socket_connect_hostname()");

  timer = evtimer_new(base, timer_cb, this);
}

Connection::~Connection() {
  event_free(timer);
  timer = NULL;

  // FIXME:  W("Drain op_q?");

  bufferevent_free(bev);

  delete iagen;
  delete keygen;
  delete keysize;
  delete valuesize;
}

void Connection::reset() {
  // FIXME: Actually check the connection, drain all bufferevents, drain op_q.
  assert(op_queue.size() == 0);
  evtimer_del(timer);
  read_state = IDLE;
  write_state = INIT_WRITE;
  stats = ConnectionStats(stats.sampling);
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

void Connection::pop_op() {
  assert(op_queue.size() > 0);

  op_queue.pop();

  if (read_state == LOADING) return;
  read_state = IDLE;

  // Advance the read state machine.
  if (op_queue.size() > 0) {
    Operation& op = op_queue.front();
    switch (op.type) {
    case Operation::GET: read_state = WAITING_FOR_GET; break;
    case Operation::SET: read_state = WAITING_FOR_SET; break;
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
      if (op_queue.size() >= (size_t) options.depth) {
        write_state = WAITING_FOR_OPQ;
        return;
      } else if (now < next_time) {
        write_state = WAITING_FOR_TIME;
        break; // We want to run through the state machine one more time
               // to make sure the timer is armed.
      }

      issue_something(now);
      stats.log_op(op_queue.size());

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
      if (op_queue.size() >= (size_t) options.depth) return;
      write_state = ISSUING;
      break;

    default: DIE("Not implemented");
    }
  }
}

void Connection::event_callback(short events) {
  //  struct timeval now_tv;
  // event_base_gettimeofday_cached(base, &now_tv);

  if (events & BEV_EVENT_CONNECTED) {
    D("Connected to %s:%s.", hostname.c_str(), port.c_str());
    int fd = bufferevent_getfd(bev);
    if (fd < 0) DIE("bufferevent_getfd");

    if (!options.no_nodelay) {
      int one = 1;
      if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     (void *) &one, sizeof(one)) < 0)
        DIE("setsockopt()");
    }

    if (!username_given) {
      read_state = IDLE;  // This is the most important part!
    } else {
      issue_sasl();
    }
  } else if (events & BEV_EVENT_ERROR) {
    int err = bufferevent_socket_get_dns_error(bev);
    if (err) DIE("DNS error: %s", evutil_gai_strerror(err));

    DIE("BEV_EVENT_ERROR: %s", strerror(errno));
  } else if (events & BEV_EVENT_EOF) {
    DIE("Unexpected EOF from server.");
  }
}

void Connection::write_callback() {}
void Connection::timer_callback() { drive_write_machine(); }

// The follow are C trampolines for libevent callbacks.
void bev_event_cb(struct bufferevent *bev, short events, void *ptr) {
  Connection* conn = (Connection*) ptr;
  conn->event_callback(events);
}

void bev_read_cb(struct bufferevent *bev, void *ptr) {
  Connection* conn = (Connection*) ptr;
  conn->read_callback();
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
  if (bufferevent_priority_set(bev, pri))
    DIE("bufferevent_set_priority(bev, %d) failed", pri);
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

void Connection::issue_sasl() {
  read_state = SASL;

  ConnectionBinary::binary_header_t header = {0x80, 0x21, 0, 0, 0, 0, 0, 0, 0};
  header.key_len = htons(5);
  header.body_len = htonl(6 + username.length() + 1 + password.length());

  bufferevent_write(bev, &header, 24);
  bufferevent_write(bev, "PLAIN\0", 6);
  bufferevent_write(bev, username.c_str(), username.length() + 1);
  bufferevent_write(bev, password.c_str(), password.length());
}

