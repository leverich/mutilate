// -*- c++-mode -*-
#ifndef CONNECTION_H
#define CONNECTION_H

#include <queue>
#include <string>
#include <fstream>
#include <map>
#include <unordered_map>

#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/util.h>

#include "bipbuffer.h"
#include "AdaptiveSampler.h"
#include "cmdline.h"
#include "ConnectionOptions.h"
#include "ConnectionStats.h"
#include "Generator.h"
#include "Operation.h"
#include "util.h"
#include "blockingconcurrentqueue.h"
#include "Protocol.h"

#define OPAQUE_MAX 64000
#define hashsize(n) ((unsigned long int)1<<(n))
#define hashmask(n) (hashsize(n)-1)

#define MAX_BUFFER_SIZE 10*1024*1024
#define MAX_LEVELS 2+1

using namespace std;
using namespace moodycamel;


typedef struct _evicted_type {
    bool evicted;
    uint32_t evictedFlags;
    uint32_t serverFlags;
    uint32_t clsid;
    uint32_t evictedKeyLen;
    uint32_t evictedLen;
    char *evictedKey;
    char *evictedData;
} evicted_t;

typedef struct resp {
    uint32_t opaque;
    int opcode;
    bool found;
    evicted_t* evict;
} resp_t;


void bev_event_cb(struct bufferevent *bev, short events, void *ptr);
void bev_read_cb(struct bufferevent *bev, void *ptr);
void bev_event_cb1(struct bufferevent *bev, short events, void *ptr);
void bev_event_cb1_approx(struct bufferevent *bev, short events, void *ptr);
void bev_event_cb1_approx_batch(struct bufferevent *bev, short events, void *ptr);
void bev_read_cb1(struct bufferevent *bev, void *ptr);
void bev_read_cb1_approx(struct bufferevent *bev, void *ptr);
void bev_read_cb1_approx_batch(struct bufferevent *bev, void *ptr);
void bev_event_cb2(struct bufferevent *bev, short events, void *ptr);
void bev_event_cb2_approx(struct bufferevent *bev, short events, void *ptr);
void bev_event_cb2_approx_batch(struct bufferevent *bev, short events, void *ptr);
void bev_read_cb2(struct bufferevent *bev, void *ptr);
void bev_read_cb2_approx(struct bufferevent *bev, void *ptr);
void bev_read_cb2_approx_batch(struct bufferevent *bev, void *ptr);
void bev_write_cb(struct bufferevent *bev, void *ptr);
void bev_write_cb_m(struct bufferevent *bev, void *ptr);
void bev_write_cb_m_approx(struct bufferevent *bev, void *ptr);
void bev_write_cb_m_approx_batch(struct bufferevent *bev, void *ptr);
void timer_cb(evutil_socket_t fd, short what, void *ptr);
void timer_cb_m(evutil_socket_t fd, short what, void *ptr);
void timer_cb_m_approx(evutil_socket_t fd, short what, void *ptr);
void timer_cb_m_approx_batch(evutil_socket_t fd, short what, void *ptr);

class Protocol;

class Connection {
public:
  Connection(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname, string _port, options_t options,
             //ConcurrentQueue<string> *a_trace_queue,
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
  
  uint32_t get_cid();
  //void set_queue(ConcurrentQueue<string> *a_trace_queue);
  void set_queue(queue<string> *a_trace_queue);
  void set_lock(pthread_mutex_t* a_lock);

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

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  uint32_t opaque;
  int issue_buf_size;
  int issue_buf_n;
  unsigned char *issue_buf_pos;
  unsigned char *issue_buf;
  bool last_quiet;
  uint32_t total;
  uint32_t cid;
  int eof;


  Protocol *prot;
  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  //std::vector<std::vector<Operation>> op_queue;
  std::unordered_map<uint32_t,Operation> op_queue;

  uint32_t op_queue_size;
  pthread_mutex_t* lock;
  //ConcurrentQueue<string> *trace_queue;
  queue<string> *trace_queue;

  // state machine functions / event processing
  void pop_op(Operation *op);
  void output_op(Operation *op, int type, bool was_found);
  //void finish_op(Operation *op);
  void finish_op(Operation *op,int was_hit);
  void issue_something(double now = 0.0);
  int issue_something_trace(double now = 0.0);
  void issue_getset(double now = 0.0);
  int issue_getsetorset(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  // request functions
  void issue_sasl();
  void issue_noop(double now = 0.0);
  void issue_get(const char* key, double now = 0.0);
  int issue_get_with_len(const char* key, int valuelen, double now = 0.0, bool quiet = false);
  int issue_set(const char* key, const char* value, int length,
                 double now = 0.0, bool is_access = false);
  void issue_set_miss(const char* key, const char* value, int length);
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

class ConnectionMulti {
public:
  ConnectionMulti(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname1, string _hostname2, string _port, options_t options,
             bool sampling = true, int fd1 = -1, int fd2 = -1);

  ~ConnectionMulti();

  int do_connect();

  double start_time; // Time when this connection began operations.
  ConnectionStats stats;
  options_t options;

  bool is_ready() { return read_state == IDLE; }
  void set_priority(int pri);

  // state commands
  void start() { 
      //fprintf(stderr,"connid: %d starting...\n",cid); 
      drive_write_machine(); 
  }
  void start_loading();
  void reset();
  bool check_exit_condition(double now = 0.0);

  void event_callback1(short events);
  void event_callback2(short events);
  void read_callback1();
  void read_callback2();
  // event callbacks
  void write_callback();
  void timer_callback();
  
  int eof;
  uint32_t get_cid();
  //void set_queue(ConcurrentQueue<string> *a_trace_queue);
  int  add_to_wb_keys(string wb_key);
  int  add_to_copy_keys(string key);
  void  del_wb_keys(string wb_key);
  void  del_copy_keys(string key);
  void set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys);
  void set_queue(queue<Operation*> *a_trace_queue);
  void set_lock(pthread_mutex_t* a_lock);

private:
  string hostname1;
  string hostname2;
  string port;

  double o_percent;
  int trace_queue_n;
  struct event_base *base;
  struct evdns_base *evdns;
  struct bufferevent *bev1;
  struct bufferevent *bev2;

  struct event *timer; // Used to control inter-transmission time.
  double next_time;    // Inter-transmission time parameters.
  double last_rx;      // Used to moderate transmission rate.
  double last_tx;

  vector<string> wb_keys;
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

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  uint32_t *opaque;
  int *issue_buf_size;
  int *issue_buf_n;
  unsigned char **issue_buf_pos;
  unsigned char **issue_buf;
  bool last_quiet1;
  bool last_quiet2;
  uint32_t total;
  uint32_t cid;
  
  //std::vector<std::queue<Operation>> op_queue;
  Operation ***op_queue;
  uint32_t *op_queue_size;

  map<string,int> key_hist;

  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  pthread_mutex_t* lock;
  unordered_map<string,vector<Operation*>> *g_wb_keys;
  queue<Operation*> *trace_queue;

  // state machine functions / event processing
  void pop_op(Operation *op);
  void output_op(Operation *op, int type, bool was_found);
  //void finish_op(Operation *op);
  void finish_op(Operation *op,int was_hit);
  int issue_getsetorset(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  // request functions
  void issue_sasl();
  void issue_noop(double now = 0.0, int level = 1);
  int issue_touch(const char* key, int valuelen, double now, int level);
  int issue_delete(const char* key, double now, uint32_t flags);
  int issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_set(const char* key, const char* value, int length, double now, uint32_t flags);

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

class ConnectionMultiApprox {
public:
  ConnectionMultiApprox(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname1, string _hostname2, string _port, options_t options,
             bool sampling = true, int fd1 = -1, int fd2 = -1);

  ~ConnectionMultiApprox();

  int do_connect();

  double start_time; // Time when this connection began operations.
  ConnectionStats stats;
  options_t options;

  bool is_ready() { return read_state == IDLE; }
  void set_priority(int pri);

  // state commands
  void start() { 
      //fprintf(stderr,"connid: %d starting...\n",cid); 
      drive_write_machine(); 
  }
  void start_loading();
  void reset();
  bool check_exit_condition(double now = 0.0);

  void event_callback1(short events);
  void event_callback2(short events);
  void read_callback1();
  void read_callback2();
  // event callbacks
  void write_callback();
  void timer_callback();
  
  int eof;
  uint32_t get_cid();
  //void set_queue(ConcurrentQueue<string> *a_trace_queue);
  int  add_to_wb_keys(string wb_key);
  int  add_to_copy_keys(string key);
  int  add_to_touch_keys(string key);
  void del_wb_keys(string wb_key);
  void del_copy_keys(string key);
  void del_touch_keys(string key);
  void set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys);
  void set_queue(queue<Operation*> *a_trace_queue);
  void set_lock(pthread_mutex_t* a_lock);

private:
  string hostname1;
  string hostname2;
  string port;

  double o_percent;
  int trace_queue_n;
  struct event_base *base;
  struct evdns_base *evdns;
  struct bufferevent *bev1;
  struct bufferevent *bev2;

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

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  uint32_t *opaque;
  int *issue_buf_size;
  int *issue_buf_n;
  unsigned char **issue_buf_pos;
  unsigned char **issue_buf;
  bool last_quiet1;
  bool last_quiet2;
  uint32_t total;
  uint32_t cid;
  uint32_t gets;
  uint32_t gloc;
  uint32_t ghits;
  uint32_t sloc;
  uint32_t esets;
  uint32_t isets;
  uint32_t iloc;
  
  //std::vector<std::queue<Operation>> op_queue;
  Operation ***op_queue;
  uint32_t *op_queue_size;


  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  pthread_mutex_t* lock;
  unordered_map<string,vector<Operation*>> *g_wb_keys;
  queue<Operation*> *trace_queue;

  // state machine functions / event processing
  void pop_op(Operation *op);
  void output_op(Operation *op, int type, bool was_found);
  //void finish_op(Operation *op);
  void finish_op(Operation *op,int was_hit);
  int issue_getsetorset(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  // request functions
  void issue_sasl();
  int issue_op(Operation* op);
  void issue_noop(double now = 0.0, int level = 1);
  int issue_touch(const char* key, int valuelen, double now, int level);
  int issue_delete(const char* key, double now, uint32_t flags);
  int issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_get_with_len(Operation *pop, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_set(const char* key, const char* value, int length, double now, uint32_t flags);
  int issue_set(Operation *pop, const char* value, double now, uint32_t flags);

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

class ConnectionMultiApproxBatch {
public:
  ConnectionMultiApproxBatch(struct event_base* _base, struct evdns_base* _evdns,
             string _hostname1, string _hostname2, string _port, options_t options,
             bool sampling = true, int fd1 = -1, int fd2 = -1);

  ~ConnectionMultiApproxBatch();

  int do_connect();

  double start_time; // Time when this connection began operations.
  ConnectionStats stats;
  options_t options;

  bool is_ready() { return read_state == IDLE; }
  void set_priority(int pri);

  // state commands
  void start() { 
      //fprintf(stderr,"connid: %d starting...\n",cid); 
      drive_write_machine(); 
  }
  void start_loading();
  void reset();
  bool check_exit_condition(double now = 0.0);

  void event_callback1(short events);
  void event_callback2(short events);
  void read_callback1();
  void read_callback2();
  void read_callback1_v1();
  void read_callback2_v1();
  // event callbacks
  void write_callback();
  void timer_callback();
  
  int eof;
  uint32_t get_cid();
  //void set_queue(ConcurrentQueue<string> *a_trace_queue);
  int  add_to_wb_keys(string wb_key);
  int  add_to_copy_keys(string key);
  int  add_to_touch_keys(string key);
  void del_wb_keys(string wb_key);
  void del_copy_keys(string key);
  void del_touch_keys(string key);
  void set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys);
  void set_queue(queue<Operation*> *a_trace_queue);
  void set_lock(pthread_mutex_t* a_lock);
  int send_write_buffer(int level);
  int add_get_op_to_queue(Operation *pop, int level);
  int add_set_to_queue(Operation *pop, int level, const char *value);
  size_t handle_response_batch(unsigned char *rbuf_pos, resp_t *resp, 
                                    size_t read_bytes, size_t consumed_bytes,
                                    int level, int extra);

private:
  string hostname1;
  string hostname2;
  string port;

  double o_percent;
  int trace_queue_n;
  struct event_base *base;
  struct evdns_base *evdns;
  struct bufferevent *bev1;
  struct bufferevent *bev2;

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

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  uint32_t *opaque;
  int *issue_buf_size;
  int *issue_buf_n;
  unsigned char **issue_buf_pos;
  unsigned char **issue_buf;
  bool last_quiet1;
  bool last_quiet2;
  uint32_t total;
  uint32_t cid;
  uint32_t gets;
  uint32_t gloc;
  uint32_t ghits;
  uint32_t sloc;
  uint32_t esets;
  uint32_t isets;
  uint32_t iloc;

  uint32_t clsid_;
  uint32_t incl_;
  uint32_t buffer_size_;
  unsigned char* buffer_write[MAX_LEVELS];
  unsigned char* buffer_read[MAX_LEVELS];
  unsigned char* buffer_write_pos[MAX_LEVELS];
  unsigned char* buffer_read_pos[MAX_LEVELS];
  unsigned char* buffer_lasthdr[MAX_LEVELS];
  unsigned char* buffer_leftover[MAX_LEVELS];
  uint32_t buffer_read_n[MAX_LEVELS];
  uint32_t buffer_write_n[MAX_LEVELS];
  uint32_t buffer_read_nbytes[MAX_LEVELS];
  uint32_t buffer_write_nbytes[MAX_LEVELS];


  //std::vector<std::queue<Operation>> op_queue;
  Operation ***op_queue;
  uint32_t *op_queue_size;


  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  pthread_mutex_t* lock;
  unordered_map<string,vector<Operation*>> *g_wb_keys;
  queue<Operation*> *trace_queue;

  // state machine functions / event processing
  void pop_op(Operation *op);
  void output_op(Operation *op, int type, bool was_found);
  //void finish_op(Operation *op);
  void finish_op(Operation *op,int was_hit);
  int issue_getsetorset(double now = 0.0);
  void drive_write_machine(double now = 0.0);

  // request functions
  void issue_sasl();
  int issue_op(Operation* op);
  int issue_noop(int level = 1);
  size_t fill_read_buffer(int level, int *extra);
  int issue_touch(const char* key, int valuelen, double now, int level);
  int issue_delete(const char* key, double now, uint32_t flags);
  int issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_get_with_len(Operation *pop, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_set(const char* key, const char* value, int length, double now, uint32_t flags);
  int issue_set(Operation *pop, const char* value, double now, uint32_t flags);

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

class ConnectionMultiApproxShm {
public:
  ConnectionMultiApproxShm(options_t options, bool sampling = true);

  ~ConnectionMultiApproxShm();

  int do_connect();

  double start_time; // Time when this connection began operations.
  ConnectionStats stats;
  options_t options;

  bool is_ready() { return read_state == IDLE; }
  void set_priority(int pri);

  void start_loading();
  void reset();
  bool check_exit_condition(double now = 0.0);

  void event_callback1(short events);
  void event_callback2(short events);
  void read_callback1();
  void read_callback2();
  void read_callback1_v1();
  void read_callback2_v1();
  // event callbacks
  void write_callback();
  void timer_callback();
  
  int eof;
  uint32_t get_cid();
  //void set_queue(ConcurrentQueue<string> *a_trace_queue);
  int  add_to_wb_keys(string wb_key);
  int  add_to_copy_keys(string key);
  int  add_to_touch_keys(string key);
  void del_wb_keys(string wb_key);
  void del_copy_keys(string key);
  void del_touch_keys(string key);
  void set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys);
  void set_queue(queue<Operation*> *a_trace_queue);
  void set_lock(pthread_mutex_t* a_lock);
  int send_write_buffer(int level);
  int add_get_op_to_queue(Operation *pop, int level);
  int add_set_to_queue(Operation *pop, int level, const char *value);
  size_t handle_response_batch(unsigned char *rbuf_pos, resp_t *resp, 
                                    size_t read_bytes, size_t consumed_bytes,
                                    int level, int extra);
  void drive_write_machine_shm(double now = 0.0);
  bipbuf_t* bipbuf_in[3];
  bipbuf_t* bipbuf_out[3];
  pthread_mutex_t* lock_in[3];
  pthread_mutex_t* lock_out[3];

private:
  string hostname1;
  string hostname2;
  string port;

  double o_percent;
  int trace_queue_n;

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

  // Parameters to track progress of the data loader.
  int loader_issued, loader_completed;

  uint32_t *opaque;
  int *issue_buf_size;
  int *issue_buf_n;
  unsigned char **issue_buf_pos;
  unsigned char **issue_buf;
  bool last_quiet1;
  bool last_quiet2;
  uint32_t total;
  uint32_t cid;
  uint32_t gets;
  uint32_t gloc;
  uint32_t ghits;
  uint32_t sloc;
  uint32_t esets;
  uint32_t isets;
  uint32_t iloc;


  //std::vector<std::queue<Operation>> op_queue;
  Operation ***op_queue;
  uint32_t *op_queue_size;


  Generator *valuesize;
  Generator *keysize;
  KeyGenerator *keygen;
  Generator *iagen;
  pthread_mutex_t* lock;
  unordered_map<string,vector<Operation*>> *g_wb_keys;
  queue<Operation*> *trace_queue;

  // state machine functions / event processing
  void pop_op(Operation *op);
  void output_op(Operation *op, int type, bool was_found);
  //void finish_op(Operation *op);
  void finish_op(Operation *op,int was_hit);
  int issue_getsetorset(double now = 0.0);

  // request functions
  void issue_sasl();
  int issue_op(Operation* op);
  void issue_noop(int level = 1);
  size_t fill_read_buffer(int level, int *extra);
  int issue_touch(const char* key, int valuelen, double now, int level);
  int issue_delete(const char* key, double now, uint32_t flags);
  int issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_get_with_len(Operation *pop, double now, bool quiet, uint32_t flags, Operation *l1 = NULL);
  int issue_set(const char* key, const char* value, int length, double now, uint32_t flags);
  int issue_set(Operation *pop, const char* value, double now, uint32_t flags);

  int read_response_l1(); 
  void read_response_l2();
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
