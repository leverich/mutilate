#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <pthread.h>

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
#include <fstream>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <string.h>
#include "blockingconcurrentqueue.h"

#define ITEM_DIRTY 16384
#define ITEM_INCL  4096
#define ITEM_EXCL  8192

#define LEVELS 2
#define SET_INCL(incl,flags)     \
    switch (incl) {              \
        case 1:                  \
            flags |= ITEM_INCL;  \
        case 2:                  \
            flags |= ITEM_EXCL;  \
    }   

#define GET_INCL(incl,flags) \
    if ((flags & ITEM_INCL) == 1) incl = 1; \
    else if ((flags & ITEM_EXCL) == 1) incl = 2; \
//#define DEBUGMC

using namespace moodycamel;

pthread_mutex_t cid_lock_m = PTHREAD_MUTEX_INITIALIZER;
static uint32_t connids_m = 1;
    
typedef struct _evicted_type {
    bool evicted;
    uint32_t evictedFlags;
    int evictedKeyLen;
    int evictedLen;
    char *evictedKey;
    char *evictedData;
} evicted_t;

static int get_incl(int vl) {
}

static int get_class(int vl, uint32_t kl) {
}

void ConnectionMulti::output_op(Operation *op, int type, bool found) {
    char output[1024];
    char k[256];
    char a[256];
    char s[256];
    memset(k,0,256);
    memset(a,0,256);
    memset(s,0,256);
    strcpy(k,op->key.c_str());
    switch (type) {
        case 0: //get
            sprintf(a,"issue_get");
            break;
        case 1: //set
            sprintf(a,"issue_set");
            break;
        case 2: //resp
            sprintf(a,"resp");
            break;
    }
    switch(read_state) {
        case INIT_READ:
            sprintf(s,"init");
            break;
        case CONN_SETUP:
            sprintf(s,"setup");
            break;
        case LOADING:
            sprintf(s,"load");
            break;
        case IDLE:
            sprintf(s,"idle");
            break;
        case WAITING_FOR_GET:
            sprintf(s,"waiting for get");
            break;
        case WAITING_FOR_SET:
            sprintf(s,"waiting for set");
            break;
        case WAITING_FOR_DELETE:
            sprintf(s,"waiting for del");
            break;
        case MAX_READ_STATE:
            sprintf(s,"max");
            break;
    }
    if (type == 2) {
        sprintf(output,"conn: %u, action: %s op: %s, opaque: %u, found: %d, type: %d\n",cid,a,k,op->opaque,found,op->type);
    } else {
        sprintf(output,"conn: %u, action: %s op: %s, opaque: %u, type: %d\n",cid,a,k,op->opaque,op->type);
    }
    write(2,output,strlen(output));
}

/**
 * Create a new connection to a server endpoint.
 */
ConnectionMulti::ConnectionMulti(struct event_base* _base1, struct event_base* _base2, struct evdns_base* _evdns,
                       string _hostname1, string _hostname2, string _port, options_t _options,
                       bool sampling ) :
  start_time(0), stats(sampling), options(_options),
  hostname1(_hostname1), hostname2(_hostname2), port(_port), base1(_base1), base2(_base2), evdns(_evdns)
{
  valuesize = createGenerator(options.valuesize);
  keysize = createGenerator(options.keysize);

  keygen = new KeyGenerator(keysize, options.records);
  
  total = 0;
  eof = 0;

  if (options.lambda <= 0) {
    iagen = createGenerator("0");
  } else {
    D("iagen = createGenerator(%s)", options.ia);
    iagen = createGenerator(options.ia);
    iagen->set_lambda(options.lambda);
  }

  read_state  = INIT_READ;
  write_state = INIT_WRITE;
  last_quiet1 = false;
  last_quiet2 = false;
  
  last_tx = last_rx = 0.0;

  pthread_mutex_lock(&cid_lock_m);
  cid = connids_m++;
  pthread_mutex_unlock(&cid_lock_m);
  
  op_queue_size = (uint32_t*)malloc(sizeof(uint32_t)*(LEVELS+1));
  opaque = (uint32_t*)malloc(sizeof(uint32_t)*(LEVELS+1));
  
  issue_buf_n = (int*)malloc(sizeof(int)*(LEVELS+1));
  issue_buf_size = (int*)malloc(sizeof(int)*(LEVELS+1));
  issue_buf = (unsigned char**)malloc(sizeof(unsigned char*)*(LEVELS+1));
  issue_buf_pos = (unsigned char**)malloc(sizeof(unsigned char*)*(LEVELS+1));

  for (int i = 1; i <= LEVELS; i++) {
      op_queue_size[i] = 0;
      opaque[i] = 0;

      issue_buf[i] = (unsigned char*)malloc(sizeof(unsigned char)*MAX_BUFFER_SIZE);
      std::unordered_map<uint32_t,Operation> op_q;
      op_queue.push_back(op_q);
      issue_buf_pos[i] = issue_buf[i];
      issue_buf_size[i] = 0;

  }
  
  timer = evtimer_new(base1, timer_cb, this);

}


void ConnectionMulti::set_queue(queue<string>* a_trace_queue) {
    trace_queue = a_trace_queue;
}

void ConnectionMulti::set_lock(pthread_mutex_t* a_lock) {
    lock = a_lock;
}

uint32_t ConnectionMulti::get_cid() {
    return cid;
}

int ConnectionMulti::do_connect() {

  int connected = 0;
  if (options.unix_socket) {
  
    bev1 = bufferevent_socket_new(base1, -1, BEV_OPT_CLOSE_ON_FREE);
    bufferevent_setcb(bev1, bev_read_cb1, bev_write_cb, bev_event_cb1, this);
    bufferevent_enable(bev1, EV_READ | EV_WRITE);
    
    bev2 = bufferevent_socket_new(base2, -1, BEV_OPT_CLOSE_ON_FREE);
    bufferevent_setcb(bev2, bev_read_cb2, bev_write_cb, bev_event_cb2, this);
    bufferevent_enable(bev2, EV_READ | EV_WRITE);

    struct sockaddr_un sin1;
    memset(&sin1, 0, sizeof(sin1));
    sin1.sun_family = AF_LOCAL;
    strcpy(sin1.sun_path, hostname1.c_str());

    int addrlen;
    addrlen = sizeof(sin1);
    int err = bufferevent_socket_connect(bev1,  (struct sockaddr*)&sin1, addrlen);
    if (err == 0) {
        connected = 1;
    } else {
	connected = 0;
        err = errno;
	fprintf(stderr,"error %s\n",strerror(err));
        bufferevent_free(bev1);
    }
    
    struct sockaddr_un sin2;
    memset(&sin2, 0, sizeof(sin2));
    sin2.sun_family = AF_LOCAL;
    strcpy(sin2.sun_path, hostname2.c_str());

    addrlen = sizeof(sin2);
    err = bufferevent_socket_connect(bev2,  (struct sockaddr*)&sin2, addrlen);
    if (err == 0) {
        connected = 1;
    } else {
	connected = 0;
        err = errno;
	fprintf(stderr,"error %s\n",strerror(err));
        bufferevent_free(bev2);
    }
  } 
  return connected;
}

/**
 * Destroy a connection, performing cleanup.
 */
ConnectionMulti::~ConnectionMulti() {
 
  event_free(timer);
  timer = NULL;
  // FIXME:  W("Drain op_q?");
  bufferevent_free(bev1);
  bufferevent_free(bev2);

  delete iagen;
  delete keygen;
  delete keysize;
  delete valuesize;
}

/**
 * Reset the connection back to an initial, fresh state.
 */
void ConnectionMulti::reset() {
  // FIXME: Actually check the connection, drain all bufferevents, drain op_q.
  //assert(op_queue.size() == 0);
  //evtimer_del(timer);
  read_state = IDLE;
  write_state = INIT_WRITE;
  stats = ConnectionStats(stats.sampling);
}

/**
 * Set our event processing priority.
 */
void ConnectionMulti::set_priority(int pri) {
  if (bufferevent_priority_set(bev1, pri)) {
    DIE("bufferevent_set_priority(bev, %d) failed", pri);
  }
}



/**
 * Get/Set or Set Style
 * If a GET command: Issue a get first, if not found then set
 * If trace file (or prob. write) says to set, then set it
 */
int ConnectionMulti::issue_getsetorset(double now) {
 

    string line;
    string rT;
    string rApp;
    string rOp;
    string rKey;
    string rKeySize;
    string rvaluelen;
    
    int ret = 0;
    int nissued = 0;
    while (nissued < options.depth) {
        
        if (trace_queue->size() > 0) {
            pthread_mutex_lock(lock);
            line = trace_queue->front();
            trace_queue->pop();
            pthread_mutex_unlock(lock);
            if (line.compare("EOF") == 0) {
                eof = 1;
                return 1;
            }
            
            stringstream ss(line);
            int Op = 0;
            int vl = 0; 
    
            if (options.twitter_trace == 1) {
                getline( ss, rT, ',' );
                getline( ss, rKey, ',' );
                getline( ss, rKeySize, ',' );
                getline( ss, rvaluelen, ',' );
                getline( ss, rApp, ',' );
                getline( ss, rOp, ',' );
                vl = stoi(rvaluelen);
                if (vl < 1) continue;
                if (vl > 524000) vl = 524000;
                if (rOp.compare("get") == 0) {
                    Op = 1;
                } else if (rOp.compare("set") == 0) {
                    Op = 2;
                } else {
                    Op = 0;
                }
                
                
            } else if (options.twitter_trace == 2) {
                getline( ss, rT, ',' );
                getline( ss, rApp, ',' );
                getline( ss, rOp, ',' );
                getline( ss, rKey, ',' );
                getline( ss, rvaluelen, ',' );
                Op = stoi(rOp);
                vl = stoi(rvaluelen);
            } else {
                getline( ss, rT, ',' );
                getline( ss, rApp, ',' );
                getline( ss, rOp, ',' );
                getline( ss, rKey, ',' );
                getline( ss, rvaluelen, ',' );
                vl = stoi(rvaluelen);
                if (rOp.compare("read") == 0) 
                    Op = 1;
                if (rOp.compare("write") == 0) 
                    Op = 2;
            }
    
    
            char key[256];
            memset(key,0,256);
            strncpy(key, rKey.c_str(),255);
            int issued = 0;
            switch(Op)
            {
              case 0:
                  //fprintf(stderr,"invalid line: %s, vl: %d @T: %d\n",
                  //        key,vl,stoi(rT));
                  break;
              case 1:
                  if (nissued < options.depth-1) {
                    issued = issue_get_with_len(key, vl, now, true, 1);
                    last_quiet1 = true;
                  } else {
                    issued = issue_get_with_len(key, vl, now, false, 1);
                    last_quiet1 = false;
                  }
                  break;
              case 2:
                  if (last_quiet1) {
                      issue_noop(now,1);
                  }
                  int index = lrand48() % (1024 * 1024);
                  int classid = 0;
                  int incl = get_incl(vl);
                  int flags = 0;
                  SET_INCL(incl,flags);
                  flags |= ITEM_DIRTY;

                  issued = issue_set(key, &random_char[index], vl, now, flags,1);
                  last_quiet1 = false;
                  break;
            
            }
            if (issued) {
                nissued++;
                total++;
            } else {
                  if (Op != 0) {
                    fprintf(stderr,"failed to issue line: %s, vl: %d @T: %d\n",
                            key,vl,stoi(rT));
                  }
                  break;
            }
        } else {
            //we should protect this with a condition variable
            //since trace queue size is 0 and not EOF.
            return 0;
        }
    }
    if (last_quiet1) {
        issue_noop(now,1);
        last_quiet1 = false;
    }
#ifdef DEBUGMC
    fprintf(stderr,"getsetorset issuing %d reqs last quiet %d\n",issue_buf_n,last_quiet);
    char *output = (char*)malloc(sizeof(char)*(issue_buf_size+512));
    fprintf(stderr,"-------------------------------------\n");
    memcpy(output,issue_buf,issue_buf_size);
    write(2,output,issue_buf_size);
    fprintf(stderr,"\n-------------------------------------\n");
    free(output);
#endif
    //buffer is ready to go!
    bufferevent_write(bev1, issue_buf[1], issue_buf_size[1]);
    
    memset(issue_buf[1],0,issue_buf_size[1]);
    issue_buf_pos[1] = issue_buf[1];
    issue_buf_size[1] = 0;
    issue_buf_n[1] = 0;

    return ret;

}

/**
 * Issue a get request to the server.
 */
int ConnectionMulti::issue_get_with_len(const char* key, int valuelen, double now, bool quiet, int level) {
  Operation op;

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base1, &now_tv);
    op.start_time = tv_to_double(&now_tv);
#else
    op.start_time = get_time();
#endif
  } else {
    op.start_time = now;
  }
#endif

  op.key = string(key);
  op.valuelen = valuelen;
  op.type = Operation::GET;
  op.opaque = opaque[level]++;
  op.level = level;
  op_queue[level][op.opaque] = op;
  op_queue_size[level]++;
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 0;
  }

  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, {htons(0)},
                        htonl(keylen) };
  if (quiet) {
      h.opcode = CMD_GETQ;
  }
  h.opaque = htonl(op.opaque);

  memcpy(issue_buf_pos[level],&h,24);
  issue_buf_pos[level] += 24;
  issue_buf_size[level] += 24;
  memcpy(issue_buf_pos[level],key,keylen);
  issue_buf_pos[level] += keylen;
  issue_buf_size[level] += keylen;
  issue_buf_n[level]++;
  
  if (read_state != LOADING) {
      stats.tx_bytes += 24 + keylen;
  }
  
  stats.log_access(op);
  return 1;
}

/**
 * Issue a get request to the server.
 */
int ConnectionMulti::issue_touch(const char* key, int valuelen, double now, int level) {
  Operation op;

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base1, &now_tv);
    op.start_time = tv_to_double(&now_tv);
#else
    op.start_time = get_time();
#endif
  } else {
    op.start_time = now;
  }
#endif

  op.key = string(key);
  op.valuelen = valuelen;
  op.type = Operation::TOUCH;
  op.opaque = opaque[level]++;
  op.level = level;
  op_queue[level][op.opaque] = op;
  op_queue_size[level]++;
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 0;
  }

  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_TOUCH, htons(keylen),
                        0x00, 0x00, {htons(0)},
                        htonl(keylen) };
  h.opaque = htonl(op.opaque);

  memcpy(issue_buf_pos[level],&h,24);
  issue_buf_pos[level] += 24;
  issue_buf_size[level] += 24;
  memcpy(issue_buf_pos[level],key,keylen);
  issue_buf_pos[level] += keylen;
  issue_buf_size[level] += keylen;
  issue_buf_n[level]++;
  
  if (read_state != LOADING) {
      stats.tx_bytes += 24 + keylen;
  }
  
  stats.log_access(op);
  return 1;
}

/**
 * Issue a delete request to the server.
 */
int ConnectionMulti::issue_delete(const char* key, double now, int level) {
  Operation op;

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base1, &now_tv);
    op.start_time = tv_to_double(&now_tv);
#else
    op.start_time = get_time();
#endif
  } else {
    op.start_time = now;
  }
#endif

  op.key = string(key);
  op.type = Operation::DELETE;
  op.opaque = opaque[level]++;
  op.level = level;
  op_queue[level][op.opaque] = op;
  op_queue_size[level]++;
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 0;
  }

  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_DELETE, htons(keylen),
                        0x00, 0x00, {htons(0)},
                        htonl(keylen) };
  h.opaque = htonl(op.opaque);

  memcpy(issue_buf_pos[level],&h,24);
  issue_buf_pos[level] += 24;
  issue_buf_size[level] += 24;
  memcpy(issue_buf_pos[level],key,keylen);
  issue_buf_pos[level] += keylen;
  issue_buf_size[level] += keylen;
  issue_buf_n[level]++;
  
  if (read_state != LOADING) {
      stats.tx_bytes += 24 + keylen;
  }
  
  stats.log_access(op);
  return 1;
}

void ConnectionMulti::issue_noop(double now, int level) {
    Operation op;
    
    if (now == 0.0) op.start_time = get_time();
    else op.start_time = now;

    binary_header_t h = { 0x80, CMD_NOOP, 0x0000,
                          0x00, 0x00, {htons(0)},
                          0x00 };

    memcpy(issue_buf_pos[level],&h,24);
    issue_buf_pos[level] += 24;
    issue_buf_size[level] += 24;
    issue_buf_n[level]++;
}

/**
 * Issue a set request to the server.
 */
int ConnectionMulti::issue_set(const char* key, const char* value, int length,
                           double now, int flags, int level) {
  Operation op; 

#if HAVE_CLOCK_GETTIME
  op.start_time = get_time_accurate();
#else
  if (now == 0.0) op.start_time = get_time();
  else op.start_time = now;
#endif

  
  op.key = string(key);
  op.valuelen = length;
  op.type = Operation::SET;
  op.opaque = opaque[level]++;
  op.level = level;
  GET_INCL(op.incl,flags);
  op.clsid = get_class(length,strlen(key));
  op_queue[level][op.opaque] = op;
  op_queue_size[level]++;
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 0;
  }

  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, {htons(0)},
                        htonl(keylen + 8 + length) }; 
  h.opaque = htonl(op.opaque);
  
  memcpy(issue_buf_pos[level],&h,24);
  issue_buf_pos[level] += 24;
  issue_buf_size[level] += 24;

  uint32_t f = htonl(flags);
  memcpy(issue_buf_pos[level],&f,4);
  issue_buf_pos[level] += 4;
  issue_buf_size[level] += 4;
  
  uint32_t exp = 0;
  memcpy(issue_buf_pos[level],&exp,4);
  issue_buf_pos[level] += 4;
  issue_buf_size[level] += 4;

  memcpy(issue_buf_pos[level],key,keylen);
  issue_buf_pos[level] += keylen;
  issue_buf_size[level] += keylen;
  
  memcpy(issue_buf_pos[level],value,length);
  issue_buf_pos[level] += length;
  issue_buf_size[level] += length;
  issue_buf_n[level]++;


  if (read_state != LOADING) {
      stats.tx_bytes += length + 32 + keylen;
  }

  stats.log_access(op);
  return 1;
}

/**
 * Return the oldest live operation in progress.
 */
void ConnectionMulti::pop_op(Operation *op) {

  uint32_t opopq = op->opaque;
  uint8_t level = op->level;
  op_queue[level].erase(opopq);
  op_queue_size[level]--;
  

  if (read_state == LOADING) return;
  read_state = IDLE;

  // Advance the read state machine.
  //if (op_queue.size() > 0) {
  //  Operation& op = op_queue.front();
  //  switch (op.type) {
  //  case Operation::GET: read_state = WAITING_FOR_GET; break;
  //  case Operation::SET: read_state = WAITING_FOR_SET; break;
  //  case Operation::DELETE: read_state = WAITING_FOR_DELETE; break;
  //  default: DIE("Not implemented.");
  //  }
  //}
}

/**
 * Finish up (record stats) an operation that just returned from the
 * server.
 */
void ConnectionMulti::finish_op(Operation *op, int was_hit) {
  double now;
#if USE_CACHED_TIME
  struct timeval now_tv;
  event_base_gettimeofday_cached(base, &now_tv);
  now = tv_to_double(&now_tv);
#else
  now = get_time();
#endif
#if HAVE_CLOCK_GETTIME
  op->end_time = get_time_accurate();
#else
  op->end_time = now;
#endif

  if (options.successful_queries && was_hit) { 
    switch (op->type) {
    case Operation::GET: stats.log_get(*op); break;
    case Operation::SET: stats.log_set(*op); break;
    case Operation::DELETE: break;
    case Operation::TOUCH: break;
    default: DIE("Not implemented.");
    }
  } else {
    switch (op->type) {
    case Operation::GET: stats.log_get(*op); break;
    case Operation::SET: stats.log_set(*op); break;
    case Operation::DELETE: break;
    case Operation::TOUCH: break;
    default: DIE("Not implemented.");
    }
  }

  last_rx = now;
  uint32_t opopq = op->opaque;
  uint8_t level = op->level;
  op_queue[level].erase(opopq);
  op_queue_size[level]--;
  read_state = IDLE;

  //lets check if we should output stats for the window
  //Do the binning for percentile outputs
  //crude at start
  if ((options.misswindow != 0) && ( ((stats.window_accesses) % options.misswindow) == 0))
  {
      if (stats.window_gets != 0)
      {
        //printf("%lu,%.4f\n",(stats.accesses),
        //        ((double)stats.window_get_misses/(double)stats.window_accesses));
        stats.window_gets = 0;
        stats.window_get_misses = 0;
        stats.window_sets = 0;
        stats.window_accesses = 0;
      }
  }

}



/**
 * Check if our testing is done and we should exit.
 */
bool ConnectionMulti::check_exit_condition(double now) {
  if (read_state == INIT_READ) return false;
  if (now == 0.0) now = get_time();

  if (options.read_file) {
    if (eof) {
        return true;
    }
    else if ((options.queries == 1) && 
        (now > start_time + options.time))
    {
        return true;
    }
    else {
        return false;
    }

  } else {
    if (options.queries != 0 && 
       (((long unsigned)options.queries) == (stats.accesses))) 
    {
        return true;
    }
    if ((options.queries == 0) && 
        (now > start_time + options.time))
    {
        return true;
    }
    if (options.loadonly && read_state == IDLE) return true;
  }

  return false;
}

/**
 * Handle new connection and error events.
 */
void ConnectionMulti::event_callback1(short events) {
  if (events & BEV_EVENT_CONNECTED) {
    D("Connected to %s:%s.", hostname1.c_str(), port.c_str());
    int fd = bufferevent_getfd(bev1);
    if (fd < 0) DIE("bufferevent_getfd");

    if (!options.no_nodelay && !options.unix_socket) {
      int one = 1;
      if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     (void *) &one, sizeof(one)) < 0)
        DIE("setsockopt()");
    }

    drive_write_machine(); 

  } else if (events & BEV_EVENT_ERROR) {
    int err = bufferevent_socket_get_dns_error(bev1);
    //if (err) DIE("DNS error: %s", evutil_gai_strerror(err));
    if (err) fprintf(stderr,"DNS error: %s", evutil_gai_strerror(err));
    fprintf(stderr,"Got an error: %s\n",
        evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

    DIE("BEV_EVENT_ERROR: %s", strerror(errno));

  } else if (events & BEV_EVENT_EOF) {
    fprintf(stderr,"Unexpected EOF from server.");
    return;
  }
}

/**
 * Handle new connection and error events.
 */
void ConnectionMulti::event_callback2(short events) {
  if (events & BEV_EVENT_CONNECTED) {
    D("Connected to %s:%s.", hostname2.c_str(), port.c_str());
    int fd = bufferevent_getfd(bev2);
    if (fd < 0) DIE("bufferevent_getfd");

    if (!options.no_nodelay && !options.unix_socket) {
      int one = 1;
      if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     (void *) &one, sizeof(one)) < 0)
        DIE("setsockopt()");
    }


  } else if (events & BEV_EVENT_ERROR) {
    int err = bufferevent_socket_get_dns_error(bev2);
    //if (err) DIE("DNS error: %s", evutil_gai_strerror(err));
    if (err) fprintf(stderr,"DNS error: %s", evutil_gai_strerror(err));
    fprintf(stderr,"Got an error: %s\n",
        evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

    DIE("BEV_EVENT_ERROR: %s", strerror(errno));

  } else if (events & BEV_EVENT_EOF) {
    fprintf(stderr,"Unexpected EOF from server.");
    return;
  }
}

/**
 * Request generation loop. Determines whether or not to issue a new command,
 * based on timer events.
 *
 * Note that this function loops. Be wary of break vs. return.
 */
void ConnectionMulti::drive_write_machine(double now) {
  if (now == 0.0) now = get_time();

  double delay;
  struct timeval tv;

  if (check_exit_condition(now)) {
      return;
  }

  while (1) {
    switch (write_state) {
    case INIT_WRITE:
      delay = iagen->generate();
      next_time = now + delay;
      double_to_tv(delay, &tv);
      evtimer_add(timer, &tv);
      write_state = WAITING_FOR_TIME;
      write_state = ISSUING;
      break;

    case ISSUING:
      if (op_queue_size[1] >= (size_t) options.depth) {
        write_state = WAITING_FOR_OPQ;
        return;
      } else if (now < next_time) {
        write_state = WAITING_FOR_TIME;
        break; // We want to run through the state machine one more time
               // to make sure the timer is armed.
      } else if (options.moderate && now < last_rx + 0.00025) {
        write_state = WAITING_FOR_TIME;
        if (!event_pending(timer, EV_TIMEOUT, NULL)) {
          delay = last_rx + 0.00025 - now;
          double_to_tv(delay, &tv);
          evtimer_add(timer, &tv);
        }
        return;
      }

      if (options.getsetorset) {
        int ret = issue_getsetorset(now);
        if (ret) return; //if at EOF
      }
      
      last_tx = now;
      for (int i = 1; i <= 2; i++) {
        stats.log_op(op_queue_size[i]);
      }
      next_time += iagen->generate();

      if (options.skip && options.lambda > 0.0 &&
          now - next_time > 0.005000 &&
          op_queue.size() >= (size_t) options.depth) {

        while (next_time < now - 0.004000) {
          stats.skips++;
          next_time += iagen->generate();
        }
      }
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
      if (op_queue_size[1] >= (size_t) options.depth) return;
      write_state = ISSUING;
      break;

    default: DIE("Not implemented");
    }
  }
}



/**
 * Tries to consume a binary response (in its entirety) from an evbuffer.
 *
 * @param input evBuffer to read response from
 * @return  true if consumed, false if not enough data in buffer.
 */
static bool handle_response(ConnectionMulti *conn, evbuffer *input, bool &done, bool &found, int &opcode, uint32_t &opaque) {
  // Read the first 24 bytes as a header
  int length = evbuffer_get_length(input);
  if (length < 24) return false;
  binary_header_t* h =
          reinterpret_cast<binary_header_t*>(evbuffer_pullup(input, 24));
  assert(h);

  int bl = ntohl(h->body_len);
  // Not whole response
  int targetLen = 24 + bl;
  if (length < targetLen) return false;
    //fprintf(stderr,"handle resp - opcode: %u opaque: %u len: %u status: %u\n",
    //        h->opcode,ntohl(h->opaque),
    //        ntohl(h->body_len),ntohl(h->status));

  opcode = h->opcode;
  opaque = ntohl(h->opaque);
  // If something other than success, count it as a miss
  if (opcode == CMD_GET && h->status) {
      conn->stats.get_misses++;
      conn->stats.window_get_misses++;
      found = false;
  }

  
  if (bl > 0 && opcode == 1) {
    //fprintf(stderr,"set resp len: %u\n",bl);
    //void *data = malloc(bl);
    //data = evbuffer_pullup(input, bl);
    //free(data);
    evbuffer_drain(input, targetLen);
  } else {
    evbuffer_drain(input, targetLen);
  }

  conn->stats.rx_bytes += targetLen;
  done = true;
  return true;
}

/**
 * Handle incoming data (responses).
 */
void ConnectionMulti::read_callback1() {
  struct evbuffer *input = bufferevent_get_input(bev1);

  Operation *op = NULL;
  bool done, found;

  //initially assume found (for sets that may come through here)
  //is this correct? do we want to assume true in case that 
  //GET was found, but wrong value size (i.e. update value)
  found = true;

  //bool full_read = true;
  //fprintf(stderr,"read_cb start with current queue of ops: %lu and issue_buf_n: %d\n",op_queue.size(),issue_buf_n);

  //if (op_queue.size() == 0) V("Spurious read callback.");
  bool full_read = true;
  while (full_read) {
    
      
    int opcode;
    uint32_t opaque;
    evicted_t evict;

    full_read = handle_response(this,input, done, found, opcode, opaque, evict);
    if (full_read) {
        if (opcode == CMD_NOOP) {
#ifdef DEBUGMC
            char out[128];
            sprintf(out,"conn l1: %u, reading noop\n",cid);
            write(2,out,strlen(out));
#endif
            continue;
        }
        op = &op_queue[1][opaque];
#ifdef DEBUGMC
        char out[128];
        sprintf(out,"conn l1: %u, reading opaque: %u\n",cid,opaque);
        write(2,out,strlen(out));
        output_op(op,2,found);
#endif
        if (op->key.length() < 1) {
#ifdef DEBUGMC
            char out2[128];
            sprintf(out2,"conn l1: %u, bad op: %s\n",cid,op->key.c_str());
            write(2,out2,strlen(out2));
#endif
            continue;
        }
    } else {
        break;
    }
    

    switch (op->type) {
        case Operation::GET:
            if (done) {
                if ( !found && (options.getset || options.getsetorset) ) {
                    /* issue a get a l2 */
                    string key = op->key;
                    int vl = op->valuelen;
                    issue_get_with_len(key.c_str(),vl,0,false,2);
                    //probably want to finish this op somehow
                    //think about the stats

                } else {
                    if (found) {
                        if (op->incl == 1) {
                            issue_touch(op->key.c_str(),0,2);
                        }
                        finish_op(op,1);
                    } else {
                        finish_op(op,0);
                    }
                }
            } else {
                char out[128];
                sprintf(out,"conn l1: %u, not done reading, should do something",cid);
                write(2,out,strlen(out));
            }
            break;
        case Operation::SET:
            if (op->incl == 1) {
                issue_touch(op->key.c_str(),0,2);
            }
            if (evict->evicted) {
                if ((evict->evictedFlags & ITEM_INCL) == 1 && (evicted->evictedFlags & ITEM_DIRTY)) {
                    issue_set(evict->evictedKey, evict->evictedData, evict->evictedLen, 0, ITEM_INCL | ITEM_DIRTY, 2);
                } else if ((evict->evictedFlags & ITEM_EXCL) == 1) {
                    issue_set(evict->evictedKey, evict->evictedData, evict->evictedLen, 0, ITEM_EXCL, 2);
                }
            }
            finish_op(op,1);
            break;
        default: 
            fprintf(stderr,"op: %p, key: %s opaque: %u\n",(void*)op,op->key.c_str(),op->opaque);
            DIE("not implemented");
    }

  }

  double now = get_time();
  if (check_exit_condition(now)) {
      return;
  }
#ifdef DEBUGMC
  fprintf(stderr,"read_cb1 done with current queue of ops: %d and issue_buf_n: %d\n",op_queue_size,issue_buf_n);
  for (auto x : op_queue) {
      cerr << x.first << ": " << x.second.key << endl;
  }
#endif
  //buffer is ready to go!
  if (issue_buf_n[1] > 0) {
    if (last_quiet1) {
        issue_noop(now,1);
        last_quiet1 = false;
    }
#ifdef DEBUGMC
    fprintf(stderr,"read_cb1 writing %d reqs, last quiet %d\n",issue_buf_n,last_quiet);
    char *output = (char*)malloc(sizeof(char)*(issue_buf_size+512));
    fprintf(stderr,"-------------------------------------\n");
    memcpy(output,issue_buf,issue_buf_size);
    write(2,output,issue_buf_size);
    fprintf(stderr,"\n-------------------------------------\n");
    free(output);
#endif

    bufferevent_write(bev1, issue_buf[1], issue_buf_size[1]);
    memset(issue_buf[1],0,issue_buf_size[1]);
    issue_buf_pos[1] = issue_buf[1];
    issue_buf_size[1] = 0;
    issue_buf_n[1] = 0;
  }

  last_tx = now;
  stats.log_op(op_queue_size[1]);
  drive_write_machine();
  
  // update events
  //if (bev != NULL) {
  //    // no pending response (nothing to read) and output buffer empty (nothing to write)
  //    if ((op_queue.size() == 0) && (evbuffer_get_length(bufferevent_get_output(bev)) == 0)) {
  //        bufferevent_disable(bev, EV_WRITE|EV_READ);
  //    }
  //}
}

/**
 * Handle incoming data (responses).
 */
void ConnectionMulti::read_callback2() {
  struct evbuffer *input = bufferevent_get_input(bev2);

  Operation *op = NULL;
  bool done, found;

  //initially assume found (for sets that may come through here)
  //is this correct? do we want to assume true in case that 
  //GET was found, but wrong value size (i.e. update value)
  found = true;

  //bool full_read = true;
  //fprintf(stderr,"read_cb start with current queue of ops: %lu and issue_buf_n: %d\n",op_queue.size(),issue_buf_n);

  //if (op_queue.size() == 0) V("Spurious read callback.");
  bool full_read = true;
  while (full_read) {
    
      
    int opcode;
    uint32_t opaque;
    full_read = handle_response(this,input, done, found, opcode, opaque);
    if (full_read) {
        if (opcode == CMD_NOOP) {
#ifdef DEBUGMC
            char out[128];
            sprintf(out,"conn l2: %u, reading noop\n",cid);
            write(2,out,strlen(out));
#endif
            continue;
        }
        op = &op_queue[2][opaque];
#ifdef DEBUGMC
        char out[128];
        sprintf(out,"conn l2: %u, reading opaque: %u\n",cid,opaque);
        write(2,out,strlen(out));
        output_op(op,2,found);
#endif
        if (op->key.length() < 1) {
#ifdef DEBUGMC
            char out2[128];
            sprintf(out2,"conn l2: %u, bad op: %s\n",cid,op->key.c_str());
            write(2,out2,strlen(out2));
#endif
            continue;
        }
    } else {
        break;
    }
    

    switch (op->type) {
        case Operation::GET:
            if (done) {
                if ( !found && (options.getset || options.getsetorset) ) {//  &&
                    //(options.twitter_trace != 1)) {
                    char key[256];
                    string keystr = op->key;
                    strcpy(key, keystr.c_str());
                    int valuelen = op->valuelen;
                    int index = lrand48() % (1024 * 1024);
                    int incl = op->incl;
                    int flags = 0;
	            SET_INCL(incl,flags);	
                    finish_op(op,0); // sets read_state = IDLE
                    if (last_quiet1) {
                        issue_noop(0,1);
                    }
                    issue_set(key, &random_char[index], valuelen, 0, flags, 1);
                    last_quiet1 = false; 
                    if (incl == 1) {
                        if (last_quiet2) {
                            issue_noop(0,2);
                        }
                        issue_set(key, &random_char[index], valuelen, 0, flags, 2);
                        last_quiet2 = false; 
                    }
                    
                } else {
                    if (found) {
                        char key[256];
                        string keystr = op->key;
                        strcpy(key, keystr.c_str());
                        int valuelen = op->valuelen;
                        int index = lrand48() % (1024 * 1024);
                        int incl = op->incl;
                        int flags = 0;
                        SET_INCL(incl,flags);
                        //found in l2, set in l1
                        issue_set(key, &random_char[index],valuelen, 0, flags, 1);
                        if (incl == 2) {
                            //if exclusive, remove from l2
                            issue_delete(key,0,1);
                        }
                        finish_op(op,1);

                    } else {
                        finish_op(op,0);
                    }
                }
            } else {
                char out[128];
                sprintf(out,"conn l2: %u, not done reading, should do something",cid);
                write(2,out,strlen(out));
            }
            break;
        case Operation::SET:
            finish_op(op,1);
            break;
        case Operation::TOUCH:
            if (!found) {
                char key[256];
                string keystr = op->key;
                strcpy(key, keystr.c_str());
                int valuelen = op->valuelen;
                int index = lrand48() % (1024 * 1024);
                int incl = op->incl;
                int flags = 0;
                SET_INCL(incl,flags);
                // not found in l2, set in l2
                issue_set(key, &random_char[index],valuelen, 0, flags, 2);
            }
        default: 
            fprintf(stderr,"op: %p, key: %s opaque: %u\n",(void*)op,op->key.c_str(),op->opaque);
            DIE("not implemented");
    }

  }

  double now = get_time();
  if (check_exit_condition(now)) {
      return;
  }
#ifdef DEBUGMC
  fprintf(stderr,"read_cb2 done with current queue of ops: %d and issue_buf_n: %d\n",op_queue_size,issue_buf_n);
  for (auto x : op_queue) {
      cerr << x.first << ": " << x.second.key << endl;
  }
#endif
  //buffer is ready to go!
  if (issue_buf_n[2] > 0) {
    if (last_quiet2) {
        issue_noop(now,2);
        last_quiet2 = false;
    }
#ifdef DEBUGMC
    fprintf(stderr,"read_cb2 writing %d reqs, last quiet %d\n",issue_buf_n[2],last_quiet2);
    char *output = (char*)malloc(sizeof(char)*(issue_buf_size[2]+512));
    fprintf(stderr,"-------------------------------------\n");
    memcpy(output,issue_buf[2],issue_buf_size[2]);
    write(2,output,issue_buf_size[2]);
    fprintf(stderr,"\n-------------------------------------\n");
    free(output);
#endif

    bufferevent_write(bev2, issue_buf[2], issue_buf_size[2]);
    memset(issue_buf[2],0,issue_buf_size[2]);
    issue_buf_pos[2] = issue_buf[2];
    issue_buf_size[2] = 0;
    issue_buf_n[2] = 0;
  }

  last_tx = now;
  stats.log_op(op_queue_size[2]);
  //drive_write_machine();
  
  // update events
  //if (bev != NULL) {
  //    // no pending response (nothing to read) and output buffer empty (nothing to write)
  //    if ((op_queue.size() == 0) && (evbuffer_get_length(bufferevent_get_output(bev)) == 0)) {
  //        bufferevent_disable(bev, EV_WRITE|EV_READ);
  //    }
  //}
}

/**
 * Callback called when write requests finish.
 */
void ConnectionMulti::write_callback() {

    //fprintf(stderr,"loaded evbuffer with ops: %u\n",op_queue.size());
}

/**
 * Callback for timer timeouts.
 */
void ConnectionMulti::timer_callback() { 
  drive_write_machine();
}


/* The follow are C trampolines for libevent callbacks. */
void bev_event_cb1(struct bufferevent *bev, short events, void *ptr) {

  ConnectionMulti* conn = (ConnectionMulti*) ptr;
  conn->event_callback1(events);
}

/* The follow are C trampolines for libevent callbacks. */
void bev_event_cb2(struct bufferevent *bev, short events, void *ptr) {

  ConnectionMulti* conn = (ConnectionMulti*) ptr;
  conn->event_callback2(events);
}

void bev_read_cb1(struct bufferevent *bev, void *ptr) {
  ConnectionMulti* conn = (ConnectionMulti*) ptr;
  conn->read_callback1();
}

void bev_read_cb2(struct bufferevent *bev, void *ptr) {
  ConnectionMulti* conn = (ConnectionMulti*) ptr;
  conn->read_callback2();
}

void timer_cb_m(evutil_socket_t fd, short what, void *ptr) {
  ConnectionMulti* conn = (ConnectionMulti*) ptr;
  conn->timer_callback();
}

