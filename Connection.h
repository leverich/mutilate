// -*- c++-mode -*-
#ifndef CONNECTION_H
#define CONNECTION_H

#include <queue>
#include <string>
#include <fstream>
#include <map>

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
#include "blockingconcurrentqueue.h"
#include "Protocol.h"

using namespace std;
using namespace moodycamel;

void bev_event_cb(struct bufferevent *bev, short events, void *ptr);
void bev_read_cb(struct bufferevent *bev, void *ptr);
void bev_write_cb(struct bufferevent *bev, void *ptr);
void timer_cb(evutil_socket_t fd, short what, void *ptr);

class Protocol;

class Connection {
public:
  Connection(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname, string _port, options_t options,
             ConcurrentQueue<string> *a_trace_queue,
             bool sampling = true);
  ~Connection();

  int do_connect();

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
    WAITING_FOR_DELETE,
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

  //need to keep value length for read key
  map<string,string> key_len;
  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  //was the last op a miss
  char last_key[256];
  int last_miss;

  int eof;

  //trace format variables
  double r_time; // time in seconds
  int r_appid; // prefix minus ':' char
  int r_type;  //1 = get, 2 = set
  int r_ksize; //key size
  int r_vsize; //-1 or size of value if hit
  int r_key; //op->key as int
  int r_hit; //1 if hit, 0 if miss

  Protocol *prot;
  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  std::queue<Operation> op_queue;

  ConcurrentQueue<string> *trace_queue;

  // state machine functions / event processing
  void pop_op();
  //void finish_op(Operation *op);
  void finish_op(Operation *op,int was_hit);
  void issue_something(double now = 0.0);
  int issue_something_trace(double now = 0.0);
  void issue_getset(double now = 0.0);
  int issue_getsetorset(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  // request functions
  void issue_sasl();
  void issue_get(const char* key, double now = 0.0);
  void issue_get_with_len(const char* key, int valuelen, double now = 0.0);
  void issue_set(const char* key, const char* value, int length,
                 double now = 0.0, bool is_access = false);
  void issue_delete90(double now = 0.0);

  // protocol fucntions
  int set_request_ascii(const char* key, const char* value, int length);
  int set_request_binary(const char* key, const char* value, int length);
  int set_request_resp(const char* key, const char* value, int length);
  
  int get_request_ascii(const char* key);
  int get_request_binary(const char* key);
  int get_request_resp(const char* key);

  bool consume_binary_response(evbuffer *input);
  bool consume_ascii_line(evbuffer *input, bool &done);
  bool consume_resp_line(evbuffer *input, bool &done);
};

#endif
