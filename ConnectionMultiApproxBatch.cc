#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
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

//#include <folly/concurrency/UnboundedQueue.h>
//#include <folly/concurrency/ConcurrentHashMap.h>

#define ITEM_L1 1
#define ITEM_L2 2
#define LOG_OP 4
#define SRC_L1_M 8
#define SRC_L1_H 16
#define SRC_L2_M 32
#define SRC_L2_H 64
#define SRC_DIRECT_SET 128
#define SRC_L1_COPY 256
#define SRC_WB 512

#define ITEM_INCL  4096
#define ITEM_EXCL  8192
#define ITEM_DIRTY 16384
#define ITEM_SIZE_CHANGE 131072
#define ITEM_WAS_HIT 262144

#define LEVELS 2
#define SET_INCL(incl,flags)     \
    switch (incl) {              \
        case 1:                  \
            flags |= ITEM_INCL;  \
            break;               \
        case 2:                  \
            flags |= ITEM_EXCL;  \
            break;               \
                                 \
    }                            \

#define GET_INCL(incl,flags) \
    if (flags & ITEM_INCL) incl = 1; \
    else if (flags & ITEM_EXCL) incl = 2; \

//#define OP_level(op) ( ((op)->flags & ITEM_L1) ? ITEM_L1 : ITEM_L2 )
#define OP_level(op) ( (op)->flags & ~(LOG_OP | \
                                     ITEM_INCL | ITEM_EXCL | ITEM_DIRTY | \
                                     SRC_L1_M | SRC_L1_H | SRC_L2_M | SRC_L2_H | \
                                     SRC_DIRECT_SET | SRC_L1_COPY | SRC_WB ) )

#define FLAGS_level(flags) ( flags & ~(LOG_OP | \
                                     ITEM_INCL | ITEM_EXCL | ITEM_DIRTY | \
                                     SRC_L1_M | SRC_L1_H | SRC_L2_M | SRC_L2_H | \
                                     SRC_DIRECT_SET | SRC_L1_COPY | SRC_WB ) ) 

#define OP_clu(op) ( (op)->flags & ~(LOG_OP | \
                                     ITEM_L1 | ITEM_L2 | ITEM_DIRTY | \
                                     SRC_L1_M | SRC_L1_H | SRC_L2_M | SRC_L2_H | \
                                     SRC_DIRECT_SET | SRC_L1_COPY | SRC_WB ) )

#define OP_src(op) ( (op)->flags & ~(ITEM_L1 | ITEM_L2 | LOG_OP | \
                                     ITEM_INCL | ITEM_EXCL | ITEM_DIRTY ) )

#define OP_log(op) ((op)->flags & LOG_OP)
#define OP_incl(op) ((op)->flags & ITEM_INCL)
#define OP_excl(op) ((op)->flags & ITEM_EXCL)
#define OP_set_flag(op,flag) ((op))->flags |= flag;

//#define DEBUGMC
//#define DEBUGS



pthread_mutex_t cid_lock_m_approx_batch = PTHREAD_MUTEX_INITIALIZER;
static uint32_t connids_m = 1;

#define NCLASSES 40
#define CHUNK_ALIGN_BYTES 8
static int classes = 0;
static int sizes[NCLASSES+1];
static int inclusives[NCLASSES+1];



static void init_inclusives(char *inclusive_str) {
    int j = 1;
    for (int i = 0; i < (int)strlen(inclusive_str); i++) {
        if (inclusive_str[i] == '-') {
            continue;
        } else {
            inclusives[j] = inclusive_str[i] - '0';
            j++;
        }
    }
}

static void init_classes() {

    double factor = 1.25;
    //unsigned int chunk_size = 48;
    //unsigned int item_size = 24;
    unsigned int size = 96; //warning if you change this you die
    unsigned int i = 0;
    unsigned int chunk_size_max = 1048576/2;
    while (++i < NCLASSES-1) {
        if (size >= chunk_size_max / factor) {
            break;
        }
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        sizes[i] = size;
        size *= factor;
    }
    sizes[i] = chunk_size_max;
    classes = i;

}

static int get_class(int vl, uint32_t kl) {
    //warning if you change this you die
    int vsize = vl+kl+48+1+2;
    int res = 1;
    while (vsize > sizes[res])
        if (res++ == classes) { 
            //fprintf(stderr,"item larger than max class size. vsize: %d, class size: %d\n",vsize,sizes[res]);
            return -1;
        }
    return res;
}

static int get_incl(int vl, int kl) {
    int clsid = get_class(vl,kl);
    if (clsid) {
        return inclusives[clsid];
    } else {
        return -1;
    }
}


void ConnectionMultiApproxBatch::output_op(Operation *op, int type, bool found) {
    char output[1024];
    char k[256];
    char a[256];
    char s[256];
    memset(k,0,256);
    memset(a,0,256);
    memset(s,0,256);
    strncpy(k,op->key,255);
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
    size_t res = write(2,output,strlen(output));
    if (res != strlen(output)) {
        fprintf(stderr,"error outputingiii\n");
    }
}

extern unordered_map<int, double> cid_rate;
extern unordered_map<string, vector<Operation*>> copy_keys;
extern unordered_map<string, int> touch_keys;
extern unordered_map<string, vector<Operation*>> wb_keys;

extern map<string,int> g_key_hist;
extern int max_n[3];

/**
 * Create a new connection to a server endpoint.
 */
ConnectionMultiApproxBatch::ConnectionMultiApproxBatch(struct event_base* _base, struct evdns_base* _evdns,
                       string _hostname1, string _hostname2, string _port, options_t _options,
                       bool sampling, int fd1, int fd2 ) :
  start_time(0), stats(sampling), options(_options),
  hostname1(_hostname1), hostname2(_hostname2), port(_port), base(_base), evdns(_evdns)
{
  pthread_mutex_lock(&cid_lock_m_approx_batch);
  cid = connids_m++;
  if (cid == 1) {
    init_classes();
    init_inclusives(options.inclusives);
  }
  cid_rate.insert( { cid, 0 } );
  
  pthread_mutex_unlock(&cid_lock_m_approx_batch);
  
  valuesize = createGenerator(options.valuesize);
  keysize = createGenerator(options.keysize);
  srand(time(NULL));
  keygen = new KeyGenerator(keysize, options.records);
  
  total = 0;
  eof = 0;
  o_percent = 0;

  if (options.lambda <= 0) {
    iagen = createGenerator("0");
  } else {
    D("iagen = createGenerator(%s)", options.ia);
    iagen = createGenerator(options.ia);
    iagen->set_lambda(options.lambda);
  }

  read_state  = IDLE;
  write_state = INIT_WRITE;
  last_quiet1 = false;
  last_quiet2 = false;
  
  last_tx = last_rx = 0.0;
  gets = 0;
  ghits = 0;
  esets = 0;
  isets = 0;
  gloc = rand() % (10*2-1)+1;
  sloc = rand() % (10*2-1)+1;
  iloc = rand() % (10*2-1)+1;

  op_queue_size = (uint32_t*)malloc(sizeof(uint32_t)*(LEVELS+1));
  opaque = (uint32_t*)malloc(sizeof(uint32_t)*(LEVELS+1));
  op_queue = (Operation***)malloc(sizeof(Operation**)*(LEVELS+1));

  for (int i = 0; i <= LEVELS; i++) {
      op_queue_size[i] = 0;
      opaque[i] = 1;
      //op_queue[i] = (Operation*)malloc(sizeof(int)*OPAQUE_MAX);
      op_queue[i] = (Operation**)malloc(sizeof(Operation*)*(OPAQUE_MAX+1));
      for (int j = 0; j <= OPAQUE_MAX; j++) {
          op_queue[i][j] = NULL;
      }

  }

  
  bev1 = bufferevent_socket_new(base, fd1, BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev1, bev_read_cb1_approx_batch, bev_write_cb_m_approx_batch, bev_event_cb1_approx_batch, this);
  bufferevent_enable(bev1, EV_READ | EV_WRITE);
  //bufferevent_setwatermark(bev1, EV_READ, 4*1024, 0);
  
  bev2 = bufferevent_socket_new(base, fd2, BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev2, bev_read_cb2_approx_batch, bev_write_cb_m_approx_batch, bev_event_cb2_approx_batch, this);
  bufferevent_enable(bev2, EV_READ | EV_WRITE);
  //bufferevent_setwatermark(bev2, EV_READ, 4*1024, 0);
  
  timer = evtimer_new(base, timer_cb_m_approx_batch, this);

  read_state  = IDLE;
}


void ConnectionMultiApproxBatch::set_queue(queue<Operation*>* a_trace_queue) {
    trace_queue = a_trace_queue;
    trace_queue_n = a_trace_queue->size();
    Operation *Op = trace_queue->front(); 
    incl_ = get_incl(Op->valuelen,strlen(Op->key));
    clsid_ = get_class(Op->valuelen,strlen(Op->key));

    buffer_size_ = (1024*512)*options.depth;
    //setup the buffers
    //max is (valuelen + 256 + 24 + 4 + 4 )  * depth
    for (int i = 1; i <= LEVELS; i++) {
        buffer_write[i] = (unsigned char*)malloc(buffer_size_);
        buffer_read[i] = (unsigned char*)malloc(buffer_size_);
        buffer_write_n[i] = 0;
        buffer_read_n[i] = 0;
        buffer_write_nbytes[i] = 0;
        buffer_read_nbytes[i] = 0;
        buffer_write_pos[i] = buffer_write[i];
        buffer_read_pos[i] = buffer_read[i];
    }

}

void ConnectionMultiApproxBatch::set_lock(pthread_mutex_t* a_lock) {
    lock = a_lock;
}

void ConnectionMultiApproxBatch::set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys) {
    g_wb_keys = a_wb_keys;
}

uint32_t ConnectionMultiApproxBatch::get_cid() {
    return cid;
}

int ConnectionMultiApproxBatch::add_to_wb_keys(string key) {
    auto pos = wb_keys.find(key);
    if (pos == wb_keys.end()) {
        wb_keys.insert( {key, vector<Operation*>() });
        return 1;
    }
    return 2;
}

void ConnectionMultiApproxBatch::del_wb_keys(string key) {

    auto position = wb_keys.find(key);
    if (position != wb_keys.end()) {
        vector<Operation*> op_list = vector<Operation*>(position->second);
        wb_keys.erase(position);
        for (auto it = op_list.begin(); it != op_list.end(); ++it) {
            issue_op(*it);
        }
    } else {
        fprintf(stderr,"expected wb %s, got nuthin\n",key.c_str());
    }
}

int ConnectionMultiApproxBatch::add_to_copy_keys(string key) {
    auto pos = copy_keys.find(key);
    if (pos == copy_keys.end()) {
        copy_keys.insert( {key, vector<Operation*>() });
        return 1;
    }
    return 2;
}


void ConnectionMultiApproxBatch::del_copy_keys(string key) {

    auto position = copy_keys.find(key);
    if (position != copy_keys.end()) {
        vector<Operation*> op_list = vector<Operation*>(position->second);
        copy_keys.erase(position);
        for (auto it = op_list.begin(); it != op_list.end(); ++it) {
            issue_op(*it);
        }
    } else {
        fprintf(stderr,"expected copy %s, got nuthin\n",key.c_str());
    }
}

int ConnectionMultiApproxBatch::add_to_touch_keys(string key) {
    //return touch_keys.assign_if_equal( key, NULL, cid ) != NULL ? 1 : 2;
    auto pos = touch_keys.find(key);
    if (pos == touch_keys.end()) {
        touch_keys.insert( {key, cid });
        return 1;
    }
    return 2;
}


void ConnectionMultiApproxBatch::del_touch_keys(string key) {
    //touch_keys.erase(key);
    auto position = touch_keys.find(key);
    if (position != touch_keys.end()) {
        touch_keys.erase(position);
    } else {
        fprintf(stderr,"expected touch %s, got nuthin\n",key.c_str());
    }
}

int ConnectionMultiApproxBatch::issue_op(Operation *Op) {
    double now = get_time();
    int issued = 0;
    Op->clsid = get_class(Op->valuelen,strlen(Op->key));
    int flags = 0;
    int index = lrand48() % (1024 * 1024);
    int incl = inclusives[Op->clsid];
    SET_INCL(incl,flags);
    
    switch(Op->type) {

    case Operation::GET:
          issued = issue_get_with_len(Op, now, false, flags | LOG_OP | ITEM_L1);
          this->stats.gets++;
          gets++;
          this->stats.gets_cid[cid]++;
          break;
    case Operation::SET:
          if (incl == 1) {
            if (isets >= iloc) {
                const char *data = &random_char[index];
                issued = issue_set(Op, data, now, flags | LOG_OP | ITEM_L1 | SRC_DIRECT_SET);
                issued = issue_touch(Op->key,Op->valuelen,now, ITEM_L2 | SRC_DIRECT_SET);
                iloc += rand()%(10*2-1)+1;
            } else {
                issued = issue_set(Op, &random_char[index], now, flags | LOG_OP | ITEM_L1 | SRC_DIRECT_SET | ITEM_DIRTY);
            }
            isets++;
          } else if (incl == 2) {
            issued = issue_set(Op, &random_char[index], now, flags | LOG_OP | ITEM_L1 | SRC_DIRECT_SET);
            if (esets >= sloc) {
                issued = issue_delete(Op->key,now,ITEM_L2 | SRC_DIRECT_SET);
                sloc += rand()%(10*2-1)+1;
            }
            esets++;
          }
          this->stats.sets++;
          this->stats.sets_cid[cid]++;
          break;
    case Operation::DELETE:
    case Operation::TOUCH:
    case Operation::NOOP:
    case Operation::SASL:
          fprintf(stderr,"invalid line: %s, vl: %d\n",Op->key,Op->valuelen);
          break;
    
    }
    return issued;
}


int ConnectionMultiApproxBatch::do_connect() {

  int connected = 0;
  if (options.unix_socket) {
  

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
        fprintf(stderr,"l1 error %s\n",strerror(err));
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
	fprintf(stderr,"l2 error %s\n",strerror(err));
    }
  } 
  read_state  = IDLE;
  return connected;
}

/**
 * Destroy a connection, performing cleanup.
 */
ConnectionMultiApproxBatch::~ConnectionMultiApproxBatch() {
 

  for (int i = 0; i <= LEVELS; i++) {
      free(op_queue[i]);
      free(buffer_write[i]);
      free(buffer_read[i]);

  }
  
  free(op_queue_size);
  free(opaque);
  free(op_queue);
  //event_free(timer);
  //timer = NULL;
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
void ConnectionMultiApproxBatch::reset() {
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
void ConnectionMultiApproxBatch::set_priority(int pri) {
  if (bufferevent_priority_set(bev1, pri)) {
    DIE("bufferevent_set_priority(bev, %d) failed", pri);
  }
}



/**
 * Get/Set or Set Style
 * If a GET command: Issue a get first, if not found then set
 * If trace file (or prob. write) says to set, then set it
 */
int ConnectionMultiApproxBatch::issue_getsetorset(double now) {
    
    Operation *Op = trace_queue->front(); 
    if (Op->type == Operation::SASL) {
        eof = 1;
        cid_rate.insert( {cid, 100 } );
        fprintf(stderr,"cid %d done\n",cid);
        string op_queue1;
        string op_queue2;
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < OPAQUE_MAX; i++) {
                if (op_queue[j+1][i] != NULL) {
                    if (j == 0) {
                        op_queue1 = op_queue1 + "," + op_queue[j+1][i]->key;
                    } else {
                        op_queue2 = op_queue2 + "," + op_queue[j+1][i]->key;
                    }
                }
            }
        }
        send_write_buffer(1);
        send_write_buffer(2);
        fprintf(stderr,"cid %d op_queue1: %s op_queue2: %s, op_queue_size1: %d, op_queue_size2: %d\n",cid,op_queue1.c_str(),op_queue2.c_str(),op_queue_size[1],op_queue_size[2]);
        return 1;
    } 
   
    while (buffer_write_n[1] < (uint32_t)options.depth) {
        int issued = issue_op(Op);
        if (issued) {
            trace_queue->pop();
            Op = trace_queue->front();

            if (Op->type == Operation::SASL) {
                eof = 1;
                cid_rate.insert( {cid, 100 } );
                fprintf(stderr,"cid %d done\n",cid);
                string op_queue1;
                string op_queue2;
                for (int j = 0; j < 2; j++) {
                    for (int i = 0; i < OPAQUE_MAX; i++) {
                        if (op_queue[j+1][i] != NULL) {
                            if (j == 0) {
                                op_queue1 = op_queue1 + "," + op_queue[j+1][i]->key;
                            } else {
                                op_queue2 = op_queue2 + "," + op_queue[j+1][i]->key;
                            }
                        }
                    }
                }
                fprintf(stderr,"cid %d op_queue1: %s op_queue2: %s, op_queue_size1: %d, op_queue_size2: %d\n",cid,op_queue1.c_str(),op_queue2.c_str(),op_queue_size[1],op_queue_size[2]);
                send_write_buffer(1);
                send_write_buffer(2);
                return 0;
            }
            total++;
            if (issued == 2) {
                return 0;
            }
        } else {
            fprintf(stderr,"failed to issue line: %s, vl: %d\n",Op->key,Op->valuelen);
        }
    }
    
    return 0;
}

int ConnectionMultiApproxBatch::send_write_buffer(int level) {
    struct bufferevent *bev = NULL;
    switch (level) {
        case 1:
            bev = bev1;
            break;
        case 2:
            bev = bev2;
            break;
        default:
            bev = bev1;
            break;
    }
    int ret = bufferevent_write(bev,buffer_write[level],buffer_write_nbytes[level]);
    if (ret != 0) {
        fprintf(stderr,"error writing buffer! level %d, size %d\n",level,buffer_write_nbytes[level]);
    }
    //fprintf(stderr,"l%d write: %u\n",level,buffer_write_nbytes[level]);
    buffer_write_n[level] = 0;
    buffer_write_pos[level] = buffer_write[level];
    memset(buffer_write_pos[level],0,buffer_write_nbytes[level]);
    stats.tx_bytes += buffer_write_nbytes[level];
    buffer_write_nbytes[level] = 0;
    return 2;
}

int ConnectionMultiApproxBatch::add_get_op_to_queue(Operation *pop, int level) {

  op_queue[level][pop->opaque] = pop;
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing get: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,pop->key,pop->valuelen,level,pop->flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  uint16_t keylen = strlen(pop->key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, htons(0),
                        htonl(keylen) };
  //if (quiet) {
  //    h.opcode = CMD_GETQ;
  //}
  h.opaque = htonl(pop->opaque);

  memcpy(buffer_write_pos[level], &h, 24);
  buffer_write_pos[level] += 24;
  memcpy(buffer_write_pos[level], pop->key, keylen);
  buffer_write_pos[level] += keylen;
  buffer_write_n[level]++;
  buffer_write_nbytes[level] += 24 + keylen;

  int res = 1;
  if (buffer_write_n[level] == (uint32_t)options.depth) {
      res = send_write_buffer(level);
  }
  return res;
}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxBatch::issue_get_with_len(Operation *pop, double now, bool quiet, uint32_t flags, Operation *l1) {
  
  int level = FLAGS_level(flags);

  //initialize op for sending
#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base, &now_tv);
    pop->start_time = tv_to_double(&now_tv);
#else
    pop->start_time = get_time();
#endif
  } else {
    pop->start_time = now;
  }
#endif

  pop->opaque = opaque[level]++;
  pop->flags = flags;
  if (l1 != NULL) {
      pop->l1 = l1;
  }

  //put op into queue
  return add_get_op_to_queue(pop,level);
}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxBatch::issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1) {
  
  int level = FLAGS_level(flags);
  Operation *pop = new Operation();

#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base, &now_tv);
    pop->start_time = tv_to_double(&now_tv);
#else
    pop->start_time = get_time();
#endif
  } else {
    pop->start_time = now;
  }
#endif

  strncpy(pop->key,key,255);
  pop->valuelen = valuelen;
  pop->type = Operation::GET;
  pop->opaque = opaque[level]++;
  pop->flags = flags;
  pop->clsid = get_class(valuelen,strlen(key));

  if (l1 != NULL) {
      pop->l1 = l1;
  }

  return add_get_op_to_queue(pop,level);

}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxBatch::issue_touch(const char* key, int valuelen, double now, int flags) {
  
  int level = FLAGS_level(flags);
  Operation *pop = new Operation();

#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base, &now_tv);
    pop->start_time = tv_to_double(&now_tv);
#else
    pop->start_time = get_time();
#endif
  } else {
    pop->start_time = now;
  }
#endif

  strncpy(pop->key,key,255);
  pop->valuelen = valuelen;
  pop->type = Operation::TOUCH;
  pop->opaque = opaque[level]++;
  pop->flags = flags;
  op_queue[level][pop->opaque] = pop;
  //op_queue[level].push(op);
  op_queue_size[level]++;
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

#ifdef DEBUGS
  fprintf(stderr,"issing touch: %s, size: %u, level %d, flags: %d, opaque: %d\n",key,valuelen,level,flags,pop->opaque);
#endif
  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_TOUCH, htons(keylen),
                        0x04, 0x00, htons(0),
                        htonl(keylen + 4) };
  h.opaque = htonl(pop->opaque);
  
  uint32_t exp = 0;
  if (flags & ITEM_DIRTY) {
      exp = htonl(flags); 
  }
  
  memcpy(buffer_write_pos[level], &h, 24);
  buffer_write_pos[level] += 24;
  memcpy(buffer_write_pos[level], &exp, 4);
  buffer_write_pos[level] += 4;
  memcpy(buffer_write_pos[level], pop->key, keylen);
  buffer_write_pos[level] += keylen;
  buffer_write_nbytes[level] += 24 + keylen + 4;
  buffer_write_n[level]++;

  int ret = 1;
  if (buffer_write_n[level] == (uint32_t)options.depth) {
      ret = send_write_buffer(level);
  }
  
  return ret;
}

/**
 * Issue a delete request to the server.
 */
int ConnectionMultiApproxBatch::issue_delete(const char* key, double now, uint32_t flags) {
  
  int level = FLAGS_level(flags);
  Operation *pop = new Operation();

#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) {
#if USE_CACHED_TIME
    struct timeval now_tv;
    event_base_gettimeofday_cached(base, &now_tv);
    pop->start_time = tv_to_double(&now_tv);
#else
    pop->start_time = get_time();
#endif
  } else {
    pop->start_time = now;
  }
#endif

  strncpy(pop->key,key,255);
  pop->type = Operation::DELETE;
  pop->opaque = opaque[level]++;
  pop->flags = flags;
  op_queue[level][pop->opaque] = pop;
  op_queue_size[level]++;
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing delete: %s, level %d, flags: %d, opaque: %d\n",cid,key,level,flags,pop->opaque);
#endif

  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_DELETE, htons(keylen),
                        0x00, 0x00, htons(0),
                        htonl(keylen) };
  h.opaque = htonl(pop->opaque);
  
  memcpy(buffer_write_pos[level], &h, 24);
  buffer_write_pos[level] += 24;
  memcpy(buffer_write_pos[level], pop->key, keylen);
  buffer_write_pos[level] += keylen;
  buffer_write_n[level]++;
  buffer_write_nbytes[level] += 24 + keylen;
  
  int ret = 1;
  if (buffer_write_n[level] == (uint32_t)options.depth) {
      ret = send_write_buffer(level);
  }
  
  return ret;
}

int ConnectionMultiApproxBatch::issue_noop(int level) {

  binary_header_t h = { 0x80, CMD_NOOP, 0x0000,
                        0x00, 0x00, htons(0),
                        0x00 };
  memcpy(buffer_write_pos[level], &h, 24);
  buffer_write_pos[level] += 24;
  
  buffer_write_n[level]++;
  buffer_write_nbytes[level] += 24;
  
  int ret = 1;
  if (buffer_write_n[level] == (uint32_t)options.depth) {
      ret = send_write_buffer(level);
  }

  return ret;
}

int ConnectionMultiApproxBatch::add_set_to_queue(Operation *pop, int level, const char* value) {
  int length = pop->valuelen;
  
  op_queue[level][pop->opaque] = pop;
  //op_queue[level].push(op);
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing set: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,pop->key,length,level,pop->flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  uint16_t keylen = strlen(pop->key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, htons(0),
                        htonl(keylen + 8 + length) }; 
  h.opaque = htonl(pop->opaque);
  
  uint32_t f = htonl(pop->flags);
  uint32_t exp = 0;
 
  memcpy(buffer_write_pos[level], &h, 24);
  buffer_write_pos[level] += 24;
  memcpy(buffer_write_pos[level], &f, 4);
  buffer_write_pos[level] += 4;
  memcpy(buffer_write_pos[level], &exp, 4);
  buffer_write_pos[level] += 4;
  memcpy(buffer_write_pos[level], pop->key, keylen);
  buffer_write_pos[level] += keylen;
  memcpy(buffer_write_pos[level], value, length);
  buffer_write_pos[level] += length;
  buffer_write_n[level]++;
  buffer_write_nbytes[level] += length + 32 + keylen;
  
  int ret = 1;
  if (buffer_write_n[level] == (uint32_t)options.depth) {
      ret = send_write_buffer(level);
  }
  return ret;
  
}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApproxBatch::issue_set(Operation *pop, const char* value, double now, uint32_t flags) {
  
  int level = FLAGS_level(flags);

#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) pop->start_time = get_time();
  else pop->start_time = now;
#endif

  pop->opaque = opaque[level]++;
  pop->flags = flags;
  return add_set_to_queue(pop,level,value);

}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApproxBatch::issue_set(const char* key, const char* value, int length, double now, uint32_t flags) {
  
  int level = FLAGS_level(flags);
  Operation *pop = new Operation();

#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) pop->start_time = get_time();
  else pop->start_time = now;
#endif

  strncpy(pop->key,key,255); 
  pop->valuelen = length;
  pop->type = Operation::SET;
  pop->opaque = opaque[level]++;
  pop->flags = flags;
  pop->clsid = get_class(length,strlen(key));

  return add_set_to_queue(pop,level,value);
  
}


/**
 * Finish up (record stats) an operation that just returned from the
 * server.
 */
void ConnectionMultiApproxBatch::finish_op(Operation *op, int was_hit) {
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

  if (was_hit) { 
    switch (op->type) {
    case Operation::GET: 
        switch (OP_level(op)) {
            case 1:
                stats.log_get_l1(*op);
                break;
            case 2:
                stats.log_get_l2(*op);
                if (op->l1 != NULL) {
                    op->l1->end_time = now;
                    stats.log_get(*(op->l1));
                }
                break;
        }
        break;
    case Operation::SET: 
        switch (OP_level(op)) {
            case 1:
                stats.log_set_l1(*op);
                break;
            case 2:
                stats.log_set_l2(*op);
                break;
        }
        break;
    case Operation::DELETE: break;
    case Operation::TOUCH: break;
    default: DIE("Not implemented.");
    }
  }

  last_rx = now;
  uint8_t level = OP_level(op);
  if (op->l1 != NULL) {
      //delete op_queue[1][op->l1->opaque];
      op_queue[1][op->l1->opaque] = 0;
      op_queue_size[1]--;
      delete op->l1;
  }
  //op_queue[level].erase(op_queue[level].begin()+opopq);
  if (op == op_queue[level][op->opaque] && 
          op->opaque == op_queue[level][op->opaque]->opaque) {
    //delete op_queue[level][op->opaque];
    op_queue[level][op->opaque] = 0;
    delete op;
  } else {
      fprintf(stderr,"op_queue out of sync! Expected %p, got %p, opa1: %d opaq2: %d\n",
              op,op_queue[level][op->opaque],op->opaque,op_queue[level][op->opaque]->opaque);
  }
  op_queue_size[level]--;
  read_state = IDLE;

}



/**
 * Check if our testing is done and we should exit.
 */
bool ConnectionMultiApproxBatch::check_exit_condition(double now) {
  if (eof && op_queue_size[1] == 0 && op_queue_size[2] == 0) {
      return true;
  }
  if (read_state == INIT_READ) return false;

  return false;
}

/**
 * Handle new connection and error events.
 */
void ConnectionMultiApproxBatch::event_callback1(short events) {
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
#ifdef DEBUGMC
    fprintf(stderr,"libevent connected %s, fd: %u\n",hostname1.c_str(),bufferevent_getfd(bev1));
#endif


  } else if (events & BEV_EVENT_ERROR) {
    int err = bufferevent_socket_get_dns_error(bev1);
    //if (err) DIE("DNS error: %s", evutil_gai_strerror(err));
    if (err) fprintf(stderr,"DNS error: %s", evutil_gai_strerror(err));
    fprintf(stderr,"CID: %d - Got an error: %s\n",this->cid,
        evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

    //DIE("BEV_EVENT_ERROR: %s", strerror(errno));

  } else if (events & BEV_EVENT_EOF) {
    fprintf(stderr,"Unexpected EOF from server.");
    return;
  }
}

/**
 * Handle new connection and error events.
 */
void ConnectionMultiApproxBatch::event_callback2(short events) {
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
#ifdef DEBUGMC
    fprintf(stderr,"libevent connected %s, fd: %u\n",hostname2.c_str(),bufferevent_getfd(bev2));
#endif


  } else if (events & BEV_EVENT_ERROR) {
    int err = bufferevent_socket_get_dns_error(bev2);
    //if (err) DIE("DNS error: %s", evutil_gai_strerror(err));
    if (err) fprintf(stderr,"DNS error: %s", evutil_gai_strerror(err));
    fprintf(stderr,"CID: %d - Got an error: %s\n",this->cid,
        evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

    //DIE("BEV_EVENT_ERROR: %s", strerror(errno));


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
void ConnectionMultiApproxBatch::drive_write_machine(double now) {

  if (now == 0.0) now = get_time();
  double delay;
  struct timeval tv;

  int max_depth = (int)options.depth*2;
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
      write_state = ISSUING;
      break;

    case ISSUING:
      if ( (op_queue_size[1] >= (size_t) max_depth) || 
          (op_queue_size[2] >= (size_t) max_depth) ) {
        write_state = WAITING_FOR_OPQ;
        break;
      }

      if (options.getsetorset) {
        int ret = issue_getsetorset(now);
        if (ret == 1) return; //if at EOF
      }
      
      last_tx = now;
      for (int i = 1; i <= 2; i++) {
        stats.log_op(op_queue_size[i]);
      }
      break;

    case WAITING_FOR_TIME:
      write_state = ISSUING;
      break;

    case WAITING_FOR_OPQ:
      if ( (op_queue_size[1] >= (size_t) max_depth) || 
          (op_queue_size[2] >= (size_t) max_depth) ) {
          double delay = 0.01;
          struct timeval tv;
          double_to_tv(delay, &tv);
          evtimer_add(timer, &tv);
          return;
      } else {
        write_state = ISSUING;
        break;
      }

    default: DIE("Not implemented");
    }
  }
}

size_t ConnectionMultiApproxBatch::handle_response_batch(unsigned char *rbuf_pos, resp_t *resp, 
                                    size_t read_bytes, size_t consumed_bytes,
                                    int level, int extra) {
    if (rbuf_pos[0] != 129) {
        fprintf(stderr,"we don't have a valid header %u\n",rbuf_pos[0]);
        //buffer_read_pos[level] = rbuf_pos;
        //buffer_read_n[level] = 1;
        return 0;
    }
    binary_header_t* h = reinterpret_cast<binary_header_t*>(rbuf_pos);
    uint32_t bl = ntohl(h->body_len);
    uint16_t kl = ntohs(h->key_len);
    uint8_t el = h->extra_len;
    int targetLen = 24 + bl;
    if (consumed_bytes + targetLen > (read_bytes+extra)) {
        size_t need = (consumed_bytes+targetLen) - (read_bytes+extra);
        size_t have = (read_bytes+extra) - (consumed_bytes);
        //fprintf(stderr,"we don't have enough data, need %lu more bytes (targetLen: %d)\n",need,targetLen);
        buffer_read_pos[level] = buffer_read[level]; //reset to begining of buffer
        memcpy(buffer_read_pos[level],rbuf_pos,have);
        buffer_read_n[level] = have;
        return 0;
    }
    
    resp->opcode = h->opcode;
    resp->opaque = ntohl(h->opaque);
    uint16_t status = ntohs(h->status);
#ifdef DEBUGMC
    fprintf(stderr,"cid: %d handle resp from l%d - opcode: %u opaque: %u keylen: %u extralen: %u datalen: %u status: %u\n",cid,level,
    h->opcode,ntohl(h->opaque),ntohs(h->key_len),h->extra_len,
    ntohl(h->body_len),ntohs(h->status));
#endif
    // If something other than success, count it as a miss
    if (resp->opcode == CMD_GET && status == RESP_NOT_FOUND) {
        switch(level) {
            case 1:
                stats.get_misses_l1++;
                break;
            case 2:
                stats.get_misses_l2++;
                stats.get_misses++;
                stats.window_get_misses++;
                break;
        }
        resp->found = false;
    } else if (resp->opcode == CMD_SET && kl > 0) {
      //first data is extras: clsid, flags, eflags
      if (resp->evict) {
          unsigned char *buf = rbuf_pos + 24;
          resp->evict->clsid = *((uint32_t*)buf);
          resp->evict->clsid = ntohl(resp->evict->clsid);
          buf += 4;
          
          resp->evict->serverFlags = *((uint32_t*)buf);
          resp->evict->serverFlags = ntohl(resp->evict->serverFlags);
          buf += 4;
          
          resp->evict->evictedFlags = *((uint32_t*)buf);
          resp->evict->evictedFlags = ntohl(resp->evict->evictedFlags);
          buf += 4;
    
          resp->evict->evictedKeyLen = kl;
          resp->evict->evictedKey = (char*)malloc(kl+1);
          memset(resp->evict->evictedKey,0,kl+1);
          memcpy(resp->evict->evictedKey,buf,kl);
          buf += kl;
    
          resp->evict->evictedLen = bl - kl - el;
          resp->evict->evictedData = (char*)malloc(resp->evict->evictedLen);
          memcpy(resp->evict->evictedData,buf,resp->evict->evictedLen);
          resp->evict->evicted = true;
      } 
    } else if ( (resp->opcode == CMD_DELETE || resp->opcode == CMD_TOUCH) &&
                 status == RESP_NOT_FOUND) {
      resp->found = false;
    }
    this->stats.rx_bytes += targetLen; 
    return targetLen;
}

/**
 * Handle incoming data (responses).
 */
void ConnectionMultiApproxBatch::read_callback1() {

  int level = 1;
     
  int extra = buffer_read_n[level];
  buffer_read_n[level] = 0;
  
  unsigned char* rbuf_pos = buffer_read[level];

  size_t read_bytes = bufferevent_read(bev1, rbuf_pos+extra, buffer_size_);
  //fprintf(stderr,"l1 read: %lu\n",read_bytes);
  size_t consumed_bytes = 0;
  size_t batch = options.depth;
      //we have at least some data to read
  size_t nread_ops = 0;
  while (1) {
      evicted_t *evict = (evicted_t*)malloc(sizeof(evicted_t));
      memset(evict,0,sizeof(evicted_t));
      resp_t mc_resp;
      mc_resp.found = true;
      mc_resp.evict = evict;
      size_t cbytes = handle_response_batch(rbuf_pos,&mc_resp,read_bytes,consumed_bytes,level,extra);
      if (cbytes == 0) {
          break;
      }
      rbuf_pos = rbuf_pos + cbytes;
      consumed_bytes += cbytes;
      uint32_t opaque = mc_resp.opaque;
      bool found = mc_resp.found;
      
      Operation *op = op_queue[level][opaque];
#ifdef DEBUGMC
      char out[128];
      sprintf(out,"conn l1: %u, reading opaque: %u\n",cid,opaque);
      write(2,out,strlen(out));
      output_op(op,2,found);
#endif

      double now = get_time();
      int wb = 0;
      if (options.rand_admit) {
          wb = (rand() % options.rand_admit);
      }
      int vl = op->valuelen;
      int flags = OP_clu(op);
      switch (op->type) {
          case Operation::GET:
              if ( !found && (options.getset || options.getsetorset) ) {
                  /* issue a get a l2 */
                  issue_get_with_len(op->key,vl,now,false, flags | SRC_L1_M | ITEM_L2 | LOG_OP, op);
                  op->end_time = now;
                  this->stats.log_get_l1(*op);

              } else {
                  if (OP_incl(op) && ghits >= gloc) {
                      issue_touch(op->key,vl,now, ITEM_L2 | SRC_L1_H);
                      gloc += rand()%(10*2-1)+1;
                  }
                  ghits++;
                  finish_op(op,1);
              }
              break;
          case Operation::SET:
              //if (OP_src(op) == SRC_L1_COPY ||
              //    OP_src(op) == SRC_L2_M) {
              //    del_copy_keys(string(op->key));
              //}
              if (evict->evicted) {
                  string wb_key(evict->evictedKey);
                  if ((evict->evictedFlags & ITEM_INCL) && (evict->evictedFlags & ITEM_DIRTY)) {
                      //int ret = add_to_wb_keys(wb_key);
                      //if (ret == 1) {
                          issue_set(evict->evictedKey, evict->evictedData, evict->evictedLen, now, ITEM_L2 | ITEM_INCL | LOG_OP | SRC_WB | ITEM_DIRTY);
                      //}
                      this->stats.incl_wbs++;
                  } else if (evict->evictedFlags & ITEM_EXCL) {
                      //fprintf(stderr,"excl writeback %s\n",evict->evictedKey);
                      //strncpy(wb_key,evict->evictedKey,255);
                      if ( (options.rand_admit && wb == 0) ||
                           (options.threshold && (g_key_hist[wb_key] == 1)) ||
                           (options.wb_all) ) {
                          //int ret = add_to_wb_keys(wb_key);
                          //if (ret == 1) {
                              issue_set(evict->evictedKey, evict->evictedData, evict->evictedLen, now, ITEM_L2 | ITEM_EXCL | LOG_OP | SRC_WB);
                          //}
                          this->stats.excl_wbs++;
                      }
                  }
                  if (OP_src(op) == SRC_DIRECT_SET) {
                      if ( (evict->serverFlags & ITEM_SIZE_CHANGE) || ((evict->serverFlags & ITEM_WAS_HIT) == 0)) {
                          this->stats.set_misses_l1++;
                      } else if (OP_excl(op) && evict->serverFlags & ITEM_WAS_HIT) {
                          this->stats.set_excl_hits_l1++;
                      } else if (OP_incl(op) && evict->serverFlags & ITEM_WAS_HIT) {
                          this->stats.set_incl_hits_l1++;
                      }
                  }
              }
              finish_op(op,1);
              break;
          case Operation::TOUCH:
              finish_op(op,1);
              break;
          default: 
              fprintf(stderr,"op: %p, key: %s opaque: %u\n",(void*)op,op->key,op->opaque);
              DIE("not implemented");
      }

      if (evict) {
          if (evict->evictedKey) free(evict->evictedKey);
          if (evict->evictedData) free(evict->evictedData);
          free(evict);
      }
      nread_ops++;
      if (rbuf_pos[0] != 129) {
          break;
      }
  }
  //if (buffer_read_n[level] == 0) {
  //    memset(buffer_read[level],0,read_bytes);
  //}
  if (nread_ops == 0) {
      fprintf(stderr,"ugh only got: %lu ops expected %lu, read %lu, cid %u\n",nread_ops,batch,read_bytes,cid);
  }
  

  double now = get_time();
  if (check_exit_condition(now)) {
      return;
  }

  last_tx = now;
  stats.log_op(op_queue_size[1]);
  stats.log_op(op_queue_size[2]);
  drive_write_machine();
  
}

/**
 * Handle incoming data (responses).
 */
void ConnectionMultiApproxBatch::read_callback2() {
  int level = 2;
  
  int extra = buffer_read_n[level];
  buffer_read_n[level] = 0;
  unsigned char* rbuf_pos = buffer_read[level];


  size_t read_bytes = bufferevent_read(bev2, rbuf_pos+extra,buffer_size_);
  //fprintf(stderr,"l2 read: %lu\n",read_bytes);
  size_t consumed_bytes = 0;
  size_t batch = options.depth;
  size_t nread_ops = 0;
  while (1) {
      evicted_t *evict = NULL;
      resp_t mc_resp;
      mc_resp.found = true;
      mc_resp.evict = evict;
      size_t cbytes = handle_response_batch(rbuf_pos,&mc_resp,read_bytes,consumed_bytes,level,extra);
      if (cbytes == 0) {
          break;
      }
      rbuf_pos = rbuf_pos + cbytes;
      consumed_bytes += cbytes;
      uint32_t opaque = mc_resp.opaque;
      bool found = mc_resp.found;
      
      Operation *op = op_queue[level][opaque];
#ifdef DEBUGMC
      char out[128];
      sprintf(out,"conn l2: %u, reading opaque: %u\n",cid,opaque);
      write(2,out,strlen(out));
      output_op(op,2,found);
#endif
      double now = get_time();
      switch (op->type) {
          case Operation::GET:
              if ( !found && (options.getset || options.getsetorset) ) {
                  int valuelen = op->valuelen;
                  int index = lrand48() % (1024 * 1024);
                  int flags = OP_clu(op) | SRC_L2_M | LOG_OP;
                  //int ret = add_to_copy_keys(string(op->key));
                  //if (ret == 1) {
                  issue_set(op->key, &random_char[index], valuelen, now, flags | ITEM_L1);
                  if (OP_incl(op)) {
                      issue_set(op->key, &random_char[index], valuelen, now, flags | ITEM_L2);
                  }
                  //}
                  finish_op(op,0); // sets read_state = IDLE
              } else {
                  if (found) {
                      int valuelen = op->valuelen;
                      int index = lrand48() % (1024 * 1024);
                      int flags = OP_clu(op) | ITEM_L1 | SRC_L1_COPY;
                      string key = string(op->key);
                      const char *data = &random_char[index];
                      //int ret = add_to_copy_keys(string(op->key));
                      //if (ret == 1) {
                          issue_set(op->key,data,valuelen, now, flags);
                      //}
                      this->stats.copies_to_l1++;
                      //djb: this is automatically done in the L2 server
                      //if (OP_excl(op)) { //djb: todo should we delete here for approx or just let it die a slow death?
                      //    issue_delete(op->key,now, ITEM_L2 | SRC_L1_COPY );
                      //}
                      finish_op(op,1);

                  } else {
                      finish_op(op,0);
                  }
              }
              break;
          case Operation::SET:
              //if (OP_src(op) == SRC_WB) {
              //    del_wb_keys(string(op->key));
              //}
              finish_op(op,1);
              break;
          case Operation::TOUCH:
              if (OP_src(op) == SRC_DIRECT_SET || SRC_L1_H) {
                  int valuelen = op->valuelen;
                  if (!found) {
                      int index = lrand48() % (1024 * 1024);
                      issue_set(op->key, &random_char[index],valuelen,now, ITEM_INCL | ITEM_L2 | LOG_OP | SRC_L2_M);
                      this->stats.set_misses_l2++;
                  } else {
                      if (OP_src(op) == SRC_DIRECT_SET) {
                          issue_touch(op->key,valuelen,now, ITEM_L1 | SRC_L2_H | ITEM_DIRTY);
                      }
                  }
                  //del_touch_keys(string(op->key));
              }
              finish_op(op,0);
              break;
          case Operation::DELETE:
              //check to see if it was a hit
              //fprintf(stderr," del %s -- %d from %d\n",op->key.c_str(),found,OP_src(op));
              if (OP_src(op) == SRC_DIRECT_SET) {
                  if (found) {
                      this->stats.delete_hits_l2++;
                  } else {
                      this->stats.delete_misses_l2++;
                  }
              }
              finish_op(op,1);
              break;
          default: 
              fprintf(stderr,"op: %p, key: %s opaque: %u\n",(void*)op,op->key,op->opaque);
              DIE("not implemented");
      }
      nread_ops++;
      if (rbuf_pos[0] != 129) {
          break;
      }
  }
  //if (buffer_read_n[level] == 0) {
  //    memset(buffer_read[level],0,read_bytes);
  //}
  if (nread_ops == 0) {
      fprintf(stderr,"ugh l2 only got: %lu ops expected %lu\n",nread_ops,batch);
  }


  double now = get_time();
  if (check_exit_condition(now)) {
      return;
  }

  last_tx = now;
  stats.log_op(op_queue_size[2]);
  stats.log_op(op_queue_size[1]);
  drive_write_machine();
  
}

/**
 * Callback called when write requests finish.
 */
void ConnectionMultiApproxBatch::write_callback() {

    //fprintf(stderr,"loaded evbuffer with ops: %u\n",op_queue.size());
}

/**
 * Callback for timer timeouts.
 */
void ConnectionMultiApproxBatch::timer_callback() {
  //fprintf(stderr,"timer up: %d\n",cid);
  drive_write_machine();
}


/* The follow are C trampolines for libevent callbacks. */
void bev_event_cb1_approx_batch(struct bufferevent *bev, short events, void *ptr) {

  ConnectionMultiApproxBatch* conn = (ConnectionMultiApproxBatch*) ptr;
  conn->event_callback1(events);
}

/* The follow are C trampolines for libevent callbacks. */
void bev_event_cb2_approx_batch(struct bufferevent *bev, short events, void *ptr) {

  ConnectionMultiApproxBatch* conn = (ConnectionMultiApproxBatch*) ptr;
  conn->event_callback2(events);
}

void bev_read_cb1_approx_batch(struct bufferevent *bev, void *ptr) {
  ConnectionMultiApproxBatch* conn = (ConnectionMultiApproxBatch*) ptr;
  conn->read_callback1();
}


void bev_read_cb2_approx_batch(struct bufferevent *bev, void *ptr) {
  ConnectionMultiApproxBatch* conn = (ConnectionMultiApproxBatch*) ptr;
  conn->read_callback2();
}

void bev_write_cb_m_approx_batch(struct bufferevent *bev, void *ptr) {
}

void timer_cb_m_approx_batch(evutil_socket_t fd, short what, void *ptr) {
  ConnectionMultiApproxBatch* conn = (ConnectionMultiApproxBatch*) ptr;
  conn->timer_callback();
}

