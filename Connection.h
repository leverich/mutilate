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

using namespace std;

void bev_event_cb(struct bufferevent *bev, short events, void *ptr);
void bev_read_cb(struct bufferevent *bev, void *ptr);
void bev_write_cb(struct bufferevent *bev, void *ptr);
void timer_cb(evutil_socket_t fd, short what, void *ptr);

class Connection {
public:
  Connection(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname, string _port, options_t options,
             bool sampling = true);

  Connection(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname, string _port, string _username,
             string _password, options_t options, bool sampling = true);
  virtual ~Connection();

  string hostname;
  string port;
  string username;
  string password;

  double start_time;  // Time when this connection began operations.

  enum read_state_enum {
    INIT_READ,
    SASL,
    LOADING,
    IDLE,
    WAITING_FOR_GET,
    WAITING_FOR_GET_DATA,
    WAITING_FOR_END,
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

  ConnectionStats stats;

  virtual void issue_get(const char* key, double now = 0.0) = 0;
  virtual void issue_set(const char* key, const char* value, int length,
                         double now = 0.0) = 0;
  void issue_something(double now = 0.0);
  void pop_op();
  bool check_exit_condition(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  void start_loading();

  void reset();

  void event_callback(short events);
  virtual void read_callback() = 0;
  void write_callback();
  void timer_callback();

  void set_priority(int pri);

  options_t options;

  std::queue<Operation> op_queue;

protected:
  struct event_base *base;
  struct evdns_base *evdns;
  struct bufferevent *bev;

  bool username_given;

  struct event *timer;  // Used to control inter-transmission time.
  double lambda, next_time; // Inter-transmission time parameters.

  int data_length;  // When waiting for data, how much we're peeking for.

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;

  void issue_sasl();

private:
  void startup();
};

#endif // CONNECTION_H
