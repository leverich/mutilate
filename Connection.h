// -*- c++-mode -*-
#ifndef CONNECTION_H
#define CONNECTION_H

#include <queue>
#include <string>

#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/util.h>

#include "AdaptiveSampler.h"
#include "cmdline.h"
#include "ConnectionOptions.h"
#include "ConnectionStats.h"
#include "Generator.h"
#include "Operation.h"
#include "util.h"

#include "Protocol.h"

using namespace std;

void bev_event_cb(struct bufferevent *bev, short events, void *ptr);
void bev_read_cb(struct bufferevent *bev, void *ptr);
void bev_write_cb(struct bufferevent *bev, void *ptr);
void timer_cb(evutil_socket_t fd, short what, void *ptr);

class Protocol;

class Connection {
public:
  Connection(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname, string _port, options_t options,
             bool sampling = true);
  ~Connection();

  double start_time; // Time when this connection began operations.
  ConnectionStats stats;
  options_t options;

  bool is_ready() { return read_state == IDLE; }
  void set_priority(int pri);

  // state commands
  void start() { drive_write_machine(); }
  void start_loading();
  void reset();
  bool check_exit_condition(double now = 0.0);

  // event callbacks
  void event_callback(short events);
  void read_callback();
  void write_callback();
  void timer_callback();

private:
  string hostname;
  string port;

  struct event_base *base;
  struct evdns_base *evdns;
  struct bufferevent *bev;

  struct event *timer; // Used to control inter-transmission time.
  double next_time;    // Inter-transmission time parameters.
  double last_rx;      // Used to moderate transmission rate.
  double last_tx;

  enum read_state_enum {
    INIT_READ,
    CONN_SETUP,
    LOADING,
    IDLE,
    WAITING_FOR_GET,
    WAITING_FOR_SET,
    MAX_READ_STATE,
  };

  enum write_state_enum {
    INIT_WRITE,
    ISSUING,
    WAITING_FOR_TIME,
    WAITING_FOR_OPQ,
    MAX_WRITE_STATE,
  };

  read_state_enum read_state;
  write_state_enum write_state;

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  Protocol *prot;
  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  std::queue<Operation> op_queue;

  // state machine functions / event processing
  void pop_op();
  void finish_op(Operation *op);
  void issue_something(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  // request functions
  void issue_sasl();
  void issue_get(const char* key, double now = 0.0);
  void issue_set(const char* key, const char* value, int length,
                 double now = 0.0);

  // protocol fucntions
  int set_request_ascii(const char* key, const char* value, int length);
  int set_request_binary(const char* key, const char* value, int length);
  int get_request_ascii(const char* key);
  int get_request_binary(const char* key);

  bool consume_binary_response(evbuffer *input);
  bool consume_ascii_line(evbuffer *input, bool &done);
};

#endif
