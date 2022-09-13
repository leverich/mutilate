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
//using namespace folly;
using namespace moodycamel;
//using namespace fmt;

//struct node {
//    long long addr,label;
//    node *nxt;
//    node(long long _addr = 0, long long _label = 0, node *_nxt = NULL)
//         : addr(_addr),label(_label),nxt(_nxt) {}
//};
//
//struct tnode {
//    long long tm,offset; int size;
//};//trace file data structure
//
//long long find(long long addr) {
//    int t = addr%MAXH;
//    node *tmp = hash[t],*pre = NULL;
//    while (tmp) {
//        if (tmp->addr == addr) {
//            long long tlabel = tmp->label;
//            if (pre == NULL) hash[t] = tmp->nxt;
//            else pre->nxt = tmp->nxt;
//            delete tmp;
//            return tlabel;
//        }
//        pre = tmp;
//        tmp = tmp->nxt;
//    }
//    return 0;
//}
//
//void insert(long long addr )  {
//    int t = addr%MAXH;
//    node *tmp = new node(addr,n,hash[t]);
//    hash[t] = tmp;
//}



pthread_mutex_t cid_lock_m_approx = PTHREAD_MUTEX_INITIALIZER;
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

void ConnectionMultiApprox::output_op(Operation *op, int type, bool found) {
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
    write(2,output,strlen(output));
}

//extern USPMCQueue<Operation*,true,8,7> g_trace_queue;
//static vector<double> cid_rate;
//extern ConcurrentHashMap<int, double> cid_rate;
extern unordered_map<int, double> cid_rate;
//extern ConcurrentHashMap<string, vector<Operation*>> copy_keys;
extern unordered_map<string, vector<Operation*>> copy_keys;
extern unordered_map<string, int> touch_keys;
extern unordered_map<string, vector<Operation*>> wb_keys;
//extern ConcurrentHashMap<string, vector<Operation*>> wb_keys;

extern map<string,int> g_key_hist;
extern int max_n[3];

/**
 * Create a new connection to a server endpoint.
 */
ConnectionMultiApprox::ConnectionMultiApprox(struct event_base* _base, struct evdns_base* _evdns,
                       string _hostname1, string _hostname2, string _port, options_t _options,
                       bool sampling, int fd1, int fd2 ) :
  start_time(0), stats(sampling), options(_options),
  hostname1(_hostname1), hostname2(_hostname2), port(_port), base(_base), evdns(_evdns)
{
  pthread_mutex_lock(&cid_lock_m_approx);
  cid = connids_m++;
  if (cid == 1) {
    init_classes();
    init_inclusives(options.inclusives);
  }
  cid_rate.insert( { cid, 0 } );
  
  pthread_mutex_unlock(&cid_lock_m_approx);
  
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
  bufferevent_setcb(bev1, bev_read_cb1_approx, bev_write_cb_m_approx, bev_event_cb1_approx, this);
  bufferevent_enable(bev1, EV_READ | EV_WRITE);
  
  bev2 = bufferevent_socket_new(base, fd2, BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev2, bev_read_cb2_approx, bev_write_cb_m_approx, bev_event_cb2_approx, this);
  bufferevent_enable(bev2, EV_READ | EV_WRITE);
  
  timer = evtimer_new(base, timer_cb_m_approx, this);

  read_state  = IDLE;
}


void ConnectionMultiApprox::set_queue(queue<Operation*>* a_trace_queue) {
    trace_queue = a_trace_queue;
    trace_queue_n = a_trace_queue->size();
}

void ConnectionMultiApprox::set_lock(pthread_mutex_t* a_lock) {
    lock = a_lock;
}

void ConnectionMultiApprox::set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys) {
    g_wb_keys = a_wb_keys;
}

uint32_t ConnectionMultiApprox::get_cid() {
    return cid;
}

int ConnectionMultiApprox::add_to_wb_keys(string key) {
    auto pos = wb_keys.find(key);
    if (pos == wb_keys.end()) {
        wb_keys.insert( {key, vector<Operation*>() });
        return 1;
    }
    return 2;
}

int ConnectionMultiApprox::add_to_copy_keys(string key) {
    auto pos = copy_keys.find(key);
    if (pos == copy_keys.end()) {
        copy_keys.insert( {key, vector<Operation*>() });
        return 1;
    }
    return 2;
}


void ConnectionMultiApprox::del_copy_keys(string key) {

    auto position = copy_keys.find(key);
    if (position != copy_keys.end()) {
        vector<Operation*> op_list = vector<Operation*>(position->second);
        copy_keys.erase(position);
        for (auto it = op_list.begin(); it != op_list.end(); ++it) {
            issue_op(*it);
        }
    } else {
        fprintf(stderr,"expected %s, got nuthin\n",key.c_str());
    }
}

int ConnectionMultiApprox::add_to_touch_keys(string key) {
    //return touch_keys.assign_if_equal( key, NULL, cid ) != NULL ? 1 : 2;
    auto pos = touch_keys.find(key);
    if (pos == touch_keys.end()) {
        touch_keys.insert( {key, cid });
        return 1;
    }
    return 2;
}


void ConnectionMultiApprox::del_touch_keys(string key) {
    //touch_keys.erase(key);
    auto position = touch_keys.find(key);
    if (position != touch_keys.end()) {
        touch_keys.erase(position);
    } else {
        fprintf(stderr,"expected %s, got nuthin\n",key.c_str());
    }
}

int ConnectionMultiApprox::issue_op(Operation *Op) {
    double now = get_time();
    int issued = 0;
    int incl = get_incl(Op->valuelen,strlen(Op->key));
    int cid = get_class(Op->valuelen,strlen(Op->key));
    Op->clsid = cid;
    int flags = 0;
    int index = lrand48() % (1024 * 1024);
    //int touch = 1;
    SET_INCL(incl,flags);
    
    switch(Op->type)
    {
      case Operation::GET:
          //if (nissued < options.depth-1) {
          //  issued = issue_get_with_len(key, vl, now, false, 1, flags, 0, 1);
          //  last_quiet1 = false;
          //} else {
          //}
          issued = issue_get_with_len(Op, now, false, flags | LOG_OP | ITEM_L1);
          last_quiet1 = false;
          this->stats.gets++;
          gets++;
          this->stats.gets_cid[cid]++;
    
          break;
    case Operation::SET:
          if (last_quiet1) {
              issue_noop(now,1);
          }
          if (incl == 1) {
            if (isets >= iloc) {
            //if (1) {
                const char *data = &random_char[index];
                issued = issue_set(Op, data, now, flags | LOG_OP | ITEM_L1 | SRC_DIRECT_SET);
                //int ret = add_to_touch_keys(string(Op->key));
                //if (ret == 1) {
                    issue_touch(Op->key,Op->valuelen,now, ITEM_L2 | SRC_DIRECT_SET);
                //}
                iloc += rand()%(10*2-1)+1;
            } else {
                issued = issue_set(Op, &random_char[index], now, flags | LOG_OP | ITEM_L1 | SRC_DIRECT_SET | ITEM_DIRTY);
            }
            isets++;
          } else if (incl == 2) {
            issued = issue_set(Op, &random_char[index], now, flags | LOG_OP | ITEM_L1 | SRC_DIRECT_SET);
            if (esets >= sloc) {
                issue_delete(Op->key,now,ITEM_L2 | SRC_DIRECT_SET);
                sloc += rand()%(10*2-1)+1;
            }
            esets++;
          }
          last_quiet1 = false;
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

void ConnectionMultiApprox::del_wb_keys(string key) {

    auto position = wb_keys.find(key);
    if (position != wb_keys.end()) {
        vector<Operation*> op_list = vector<Operation*>(position->second);
        wb_keys.erase(position);
        for (auto it = op_list.begin(); it != op_list.end(); ++it) {
            issue_op(*it);
        }
    } else {
        fprintf(stderr,"expected %s, got nuthin\n",key.c_str());
    }
}


int ConnectionMultiApprox::do_connect() {

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
ConnectionMultiApprox::~ConnectionMultiApprox() {
 

  for (int i = 0; i <= LEVELS; i++) {
      free(op_queue[i]);

  }
  
  free(op_queue_size);
  free(opaque);
  free(op_queue);
  //event_free(timer);
  //timer = NULL;
  // FIXME:  W("Drain op_q?");
  //bufferevent_free(bev1);
  //bufferevent_free(bev2);

  delete iagen;
  delete keygen;
  delete keysize;
  delete valuesize;
}

/**
 * Reset the connection back to an initial, fresh state.
 */
void ConnectionMultiApprox::reset() {
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
void ConnectionMultiApprox::set_priority(int pri) {
  if (bufferevent_priority_set(bev1, pri)) {
    DIE("bufferevent_set_priority(bev, %d) failed", pri);
  }
}



/**
 * Get/Set or Set Style
 * If a GET command: Issue a get first, if not found then set
 * If trace file (or prob. write) says to set, then set it
 */
int ConnectionMultiApprox::issue_getsetorset(double now) {
 

    
    int ret = 0;
    int nissued = 0;
    
    //while (nissued < 1) {
    
    //pthread_mutex_lock(lock);
        //if (!trace_queue->empty()) {
            
            /* check if in global wb queue */
            //double percent = (double)total/((double)trace_queue_n) * 100;
            //if (percent > o_percent+2) {
            //    //update the percentage table and see if we should execute
            //    if (options.ratelimit) {
            //        double min_percent = 1000;
            //        auto it = cid_rate.begin();
            //        while (it != cid_rate.end()) {
            //           if (it->second < min_percent) {
            //               min_percent = it->second;
            //           }
            //           ++it;
            //        }

            //        if (percent > min_percent+2) {
            //            struct timeval tv;
            //            tv.tv_sec = 0;
            //            tv.tv_usec = 100;
            //            int good = 0;
            //            if (!event_pending(timer, EV_TIMEOUT, NULL)) {
            //                good = evtimer_add(timer, &tv);
            //            }
            //            if (good != 0) {
            //                fprintf(stderr,"eventimer is messed up!\n");
            //                return 2;
            //            }
            //            return 1;
            //        }
            //    }
            //    cid_rate.insert( {cid, percent});
            //    fprintf(stderr,"%f,%d,%.4f\n",now,cid,percent);
            //    o_percent = percent;
            //}
            //
            
            Operation *Op = trace_queue->front(); 
            //Operation *Op = g_trace_queue.dequeue(); 
            
            if (Op == NULL || trace_queue->size() <= 0 || Op->type == Operation::SASL) {
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
                return 1;
            } 
            
            trace_queue->pop();

            
            //trace_queue->pop();

            //pthread_mutex_lock(lock);
            //auto check = wb_keys.find(string(Op->key));
            //if (check != wb_keys.end()) {
            //    check->second.push_back(Op);
            //    return 0;
            //}
                //pthread_mutex_unlock(lock);
                //pthread_mutex_unlock(lock);
                //struct timeval tv;
                //double delay; 
                //delay = last_rx + 0.00025 - now;
                //double_to_tv(delay,&tv);
                //int good = 0;
                ////if (!event_pending(timer, EV_TIMEOUT, NULL)) {
                //good = evtimer_add(timer, &tv);
                ////}
                //if (good != 0) {
                //    fprintf(stderr,"eventimer is messed up in checking for key: %s\n",Op->key);
                //    return 2;
                //}
                //return 1;
            //} else {
                //pthread_mutex_unlock(lock);
            int issued = issue_op(Op);
            if (issued) {
                nissued++;
                total++;
            } else {
                fprintf(stderr,"failed to issue line: %s, vl: %d\n",Op->key,Op->valuelen);
            }
            //}

        //} else {
        //    return 1;
        //}
    //}
    //if (last_quiet1) {
    //    issue_noop(now,1);
    //    last_quiet1 = false;
    //}

    return ret;

}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApprox::issue_get_with_len(Operation *pop, double now, bool quiet, uint32_t flags, Operation *l1) {
  
  //check if op is in copy_keys (currently going to L1)
  //auto check = copy_keys.find(string(pop->key));
  //if (check != copy_keys.end()) {
  //    check->second.push_back(pop);
  //    return 1;
  //} 

  struct evbuffer *output = NULL;
  int level = 0;
  switch (FLAGS_level(flags)) {
      case 1:
          level = 1;
          output = bufferevent_get_output(bev1);
          break;
      case 2:
          level = 2;
          output = bufferevent_get_output(bev2);
          break;
  }

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

  op_queue[level][pop->opaque] = pop;
  //op_queue[level].push(op);
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing get: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,pop->key,pop->valuelen,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(pop->key);


  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, htons(0),
                        htonl(keylen) };
  if (quiet) {
      h.opcode = CMD_GETQ;
  }
  h.opaque = htonl(pop->opaque);
 

  evbuffer_add(output, &h, 24);
  evbuffer_add(output, pop->key, keylen);

  stats.tx_bytes += 24 + keylen;
  return 1;
}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApprox::issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1) {

  struct evbuffer *output = NULL;
  int level = 0;
  switch (FLAGS_level(flags)) {
      case 1:
          level = 1;
          output = bufferevent_get_output(bev1);
          break;
      case 2:
          level = 2;
          output = bufferevent_get_output(bev2);
          break;
  }
  //Operation op;
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
  op_queue[level][pop->opaque] = pop;
  //op_queue[level].push(op);
  op_queue_size[level]++;

#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing get: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,key,valuelen,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  //if (read_state == IDLE) read_state = WAITING_FOR_GET;
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, htons(0),
                        htonl(keylen) };
  if (quiet) {
      h.opcode = CMD_GETQ;
  }
  h.opaque = htonl(pop->opaque);
  
  evbuffer_add(output, &h, 24);
  evbuffer_add(output, key, keylen);

  stats.tx_bytes += 24 + keylen;
  return 1;
}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApprox::issue_touch(const char* key, int valuelen, double now, int flags) {
  struct evbuffer *output = NULL;
  int level = 0;
  switch (FLAGS_level(flags)) {
      case 1:
          level = 1;
          output = bufferevent_get_output(bev1);
          break;
      case 2:
          level = 2;
          output = bufferevent_get_output(bev2);
          break;
  }
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
  op_queue[level][pop->opaque] = pop;
  op_queue_size[level]++;
  
  pop->flags = flags;
  
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
  evbuffer_add(output, &h, 24);
  evbuffer_add(output, &exp, 4);
  evbuffer_add(output, key, keylen);

  
  stats.tx_bytes += 24 + keylen;
  
  //stats.log_access(op);
  return 1;
}

/**
 * Issue a delete request to the server.
 */
int ConnectionMultiApprox::issue_delete(const char* key, double now, uint32_t flags) {
  struct evbuffer *output = NULL;
  int level = 0;
  switch (FLAGS_level(flags)) {
      case 1:
          level = 1;
          output = bufferevent_get_output(bev1);
          break;
      case 2:
          level = 2;
          output = bufferevent_get_output(bev2);
          break;
  }
  //Operation op;
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
  //op_queue[level].push(op);
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
  
  evbuffer_add(output, &h, 24);
  evbuffer_add(output, key, keylen);

  stats.tx_bytes += 24 + keylen;
  
  //stats.log_access(op);
  return 1;
}

void ConnectionMultiApprox::issue_noop(double now, int level) {
   struct evbuffer *output = NULL;
   switch (level) {
       case 1:
           output = bufferevent_get_output(bev1);
           break;
       case 2:
           output = bufferevent_get_output(bev2);
           break;
   }
   Operation op;
   
   if (now == 0.0) op.start_time = get_time();
   else op.start_time = now;

   binary_header_t h = { 0x80, CMD_NOOP, 0x0000,
                         0x00, 0x00, htons(0),
                         0x00 };
   
   evbuffer_add(output, &h, 24);

}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApprox::issue_set(Operation *pop, const char* value, double now, uint32_t flags) {
  
  //check if op is in copy_keys (currently going to L1)
  //auto check = copy_keys.find(string(pop->key));
  //if (check != copy_keys.end()) {
  //    check->second.push_back(pop);
  //    return 1;
  //} 
  
  struct evbuffer *output = NULL;
  int level = 0;
  int length = pop->valuelen;
  switch (FLAGS_level(flags)) {
      case 1:
          level = 1;
          output = bufferevent_get_output(bev1);
          break;
      case 2:
          level = 2;
          output = bufferevent_get_output(bev2);
          break;
  }

#if HAVE_CLOCK_GETTIME
  pop->start_time = get_time_accurate();
#else
  if (now == 0.0) pop->start_time = get_time();
  else pop->start_time = now;
#endif

  pop->opaque = opaque[level]++;
  pop->flags = flags;
  op_queue[level][pop->opaque] = pop;
  //op_queue[level].push(op);
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing set: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,pop->key,length,level,flags,pop->opaque);
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
  
  uint32_t f = htonl(flags);
  uint32_t exp = 0;
  
  evbuffer_add(output, &h, 24);
  evbuffer_add(output, &f, 4);
  evbuffer_add(output, &exp, 4);
  evbuffer_add(output, pop->key, keylen);
  evbuffer_add(output, value, length);

  stats.tx_bytes += length + 32 + keylen;
  return 1;
}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApprox::issue_set(const char* key, const char* value, int length, double now, uint32_t flags) {
  
  struct evbuffer *output = NULL;
  int level = 0;
  switch (FLAGS_level(flags)) {
      case 1:
          level = 1;
          output = bufferevent_get_output(bev1);
          break;
      case 2:
          level = 2;
          output = bufferevent_get_output(bev2);
          break;
  }
  //Operation op; 
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
  op_queue[level][pop->opaque] = pop;
  //op_queue[level].push(op);
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing set: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,key,length,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, htons(0),
                        htonl(keylen + 8 + length) }; 
  h.opaque = htonl(pop->opaque);
  
  uint32_t f = htonl(flags);
  uint32_t exp = 0;
  
  evbuffer_add(output, &h, 24);
  evbuffer_add(output, &f, 4);
  evbuffer_add(output, &exp, 4);
  evbuffer_add(output, key, keylen);
  evbuffer_add(output, value, length);

  stats.tx_bytes += length + 32 + keylen;
  return 1;
}

/**
 * Return the oldest live operation in progress.
 */
void ConnectionMultiApprox::pop_op(Operation *op) {

  uint8_t level = OP_level(op);
  //op_queue[level].erase(op);
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
void ConnectionMultiApprox::finish_op(Operation *op, int was_hit) {
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
  //} else {
  //  switch (op->type) {
  //  case Operation::GET: 
  //      if (OP_log(op)) {
  //          switch (OP_level(op)) {
  //              case 1:
  //                  stats.log_get_l1(*op);
  //                  break;
  //              case 2:
  //                  stats.log_get_l2(*op);
  //                  if (op->l1 != NULL) {
  //                      op->l1->end_time = now;
  //                      stats.log_get(*(op->l1));
  //                  }
  //                  break;
  //          }
  //      }
  //      break;
  //  case Operation::SET:
  //      if (OP_log(op)) {
  //          switch (OP_level(op)) {
  //              case 1:
  //                  stats.log_set_l1(*op);
  //                  break;
  //              case 2:
  //                  stats.log_set_l2(*op);
  //                  break;
  //          }
  //      }
  //      break;
  //  case Operation::DELETE: break;
  //  case Operation::TOUCH: break;
  //  default: DIE("Not implemented.");
  //  }
  //}

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
bool ConnectionMultiApprox::check_exit_condition(double now) {
  if (eof && op_queue_size[1] == 0 && op_queue_size[2] == 0) {
      return true;
  }
  if (read_state == INIT_READ) return false;

  return false;
}

/**
 * Handle new connection and error events.
 */
void ConnectionMultiApprox::event_callback1(short events) {
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
void ConnectionMultiApprox::event_callback2(short events) {
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
void ConnectionMultiApprox::drive_write_machine(double now) {

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
      write_state = ISSUING;
      break;

    case ISSUING:
      if ( (op_queue_size[1] >= (size_t) options.depth) || 
          (op_queue_size[2] >= (size_t) options.depth) ) {
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
      if ( (op_queue_size[1] >= (size_t) options.depth) || 
          (op_queue_size[2] >= (size_t) options.depth) ) {
          //double delay = 0.01;
          //struct timeval tv;
          //double_to_tv(delay, &tv);
          //evtimer_add(timer, &tv);
          return;
      } else {
        write_state = ISSUING;
        break;
      }

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
static bool handle_response(ConnectionMultiApprox *conn, evbuffer *input, bool &done, bool &found, int &opcode, uint32_t &opaque, evicted_t *evict, int level) {
  // Read the first 24 bytes as a header
  int length = evbuffer_get_length(input);
  if (length < 24) return false;
  binary_header_t* h =
          reinterpret_cast<binary_header_t*>(evbuffer_pullup(input, 24));
  //assert(h);

  uint32_t bl = ntohl(h->body_len);
  uint16_t kl = ntohs(h->key_len);
  uint8_t el = h->extra_len;
  // Not whole response
  int targetLen = 24 + bl;
  if (length < targetLen) {
      return false;
  }

  opcode = h->opcode;
  opaque = ntohl(h->opaque);
  uint16_t status = ntohs(h->status);
#ifdef DEBUGMC
    fprintf(stderr,"cid: %d handle resp from l%d - opcode: %u opaque: %u keylen: %u extralen: %u datalen: %u status: %u\n",conn->get_cid(),level,
            h->opcode,ntohl(h->opaque),ntohs(h->key_len),h->extra_len,
            ntohl(h->body_len),ntohs(h->status));
#endif


  // If something other than success, count it as a miss
  if (opcode == CMD_GET && status == RESP_NOT_FOUND) {
      switch(level) {
          case 1:
              conn->stats.get_misses_l1++;
              break;
          case 2:
              conn->stats.get_misses_l2++;
              conn->stats.get_misses++;
              conn->stats.window_get_misses++;
              break;

      }
      found = false;
      evbuffer_drain(input, targetLen);

  } else if (opcode == CMD_SET && kl > 0) {
    //first data is extras: clsid, flags, eflags
    if (evict) {
        evbuffer_drain(input,24);
        unsigned char *buf = evbuffer_pullup(input,bl);
        

        evict->clsid = *((uint32_t*)buf);
        evict->clsid = ntohl(evict->clsid);
        buf += 4;
        
        evict->serverFlags = *((uint32_t*)buf);
        evict->serverFlags = ntohl(evict->serverFlags);
        buf += 4;
        
        evict->evictedFlags = *((uint32_t*)buf);
        evict->evictedFlags = ntohl(evict->evictedFlags);
        buf += 4;

        
        evict->evictedKeyLen = kl;
        evict->evictedKey = (char*)malloc(kl+1);
        memset(evict->evictedKey,0,kl+1);
        memcpy(evict->evictedKey,buf,kl);
        buf += kl;


        evict->evictedLen = bl - kl - el;
        evict->evictedData = (char*)malloc(evict->evictedLen);
        memcpy(evict->evictedData,buf,evict->evictedLen);
        evict->evicted = true;
        //fprintf(stderr,"class: %u, serverFlags: %u, evictedFlags: %u\n",evict->clsid,evict->serverFlags,evict->evictedFlags);
        evbuffer_drain(input,bl);
    } else {
        evbuffer_drain(input, targetLen);
    }
  } else if (opcode == CMD_TOUCH && status == RESP_NOT_FOUND) {
    found = false;
    evbuffer_drain(input, targetLen);
  } else if (opcode == CMD_DELETE && status == RESP_NOT_FOUND) {
    found = false;
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
void ConnectionMultiApprox::read_callback1() {
  struct evbuffer *input = bufferevent_get_input(bev1);

  Operation *op = NULL;
  bool done, found;

  //initially assume found (for sets that may come through here)
  //is this correct? do we want to assume true in case that 
  //GET was found, but wrong value size (i.e. update value)
  found = true;

  //if (op_queue.size() == 0) V("Spurious read callback.");
  bool full_read = true;
  while (full_read) {
    
      
    int opcode;
    uint32_t opaque;
    evicted_t *evict = (evicted_t*)malloc(sizeof(evicted_t));
    memset(evict,0,sizeof(evicted_t));

    full_read = handle_response(this,input, done, found, opcode, opaque, evict,1);
    if (full_read) {
        if (opcode == CMD_NOOP) {
#ifdef DEBUGMC
            char out[128];
            sprintf(out,"conn l1: %u, reading noop\n",cid);
            write(2,out,strlen(out));
#endif
            if (evict->evictedKey) free(evict->evictedKey);
            if (evict->evictedData) free(evict->evictedData);
            free(evict);
            continue;
        }
        op = op_queue[1][opaque];
#ifdef DEBUGMC
        char out[128];
        sprintf(out,"conn l1: %u, reading opaque: %u\n",cid,opaque);
        write(2,out,strlen(out));
        output_op(op,2,found);
#endif
        if (strlen(op->key) < 1) {
#ifdef DEBUGMC
            char out2[128];
            sprintf(out2,"conn l1: %u, bad op: %s\n",cid,op->key);
            write(2,out2,strlen(out2));
#endif
            if (evict->evictedKey) free(evict->evictedKey);
            if (evict->evictedData) free(evict->evictedData);
            free(evict);
            continue;
        }
    } else {
        if (evict) {
            if (evict->evictedKey) free(evict->evictedKey);
            if (evict->evictedData) free(evict->evictedData);
            free(evict);
        }
        break;
    }
    

    double now = get_time();
    int wb = 0;
    if (options.rand_admit) {
        wb = (rand() % options.rand_admit);
    }
    switch (op->type) {
        case Operation::GET:
            if (done) {

                int vl = op->valuelen;
                if ( !found && (options.getset || options.getsetorset) ) {
                    /* issue a get a l2 */
                    int flags = OP_clu(op);
                    issue_get_with_len(op->key,vl,now,false, flags | SRC_L1_M | ITEM_L2 | LOG_OP, op);
                    op->end_time = now;
                    this->stats.log_get_l1(*op);
                    //finish_op(op,0);

                } else {
                    if (OP_incl(op) && ghits >= gloc) {
                        //int ret = add_to_touch_keys(string(op->key));
                        //if (ret == 1) {
                            issue_touch(op->key,vl,now, ITEM_L2 | SRC_L1_H);
                        //}
                        gloc += rand()%(10*2-1)+1;
                    }
                    ghits++;
                    finish_op(op,1);
                }
            } else {
                char out[128];
                sprintf(out,"conn l1: %u, not done reading, should do something",cid);
                write(2,out,strlen(out));
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

  }
  

  double now = get_time();
  if (check_exit_condition(now)) {
      return;
  }

  last_tx = now;
  stats.log_op(op_queue_size[1]);
  stats.log_op(op_queue_size[2]);
  //for (int i = 1; i <= 2; i++) {
  //    fprintf(stderr,"max issue buf n[%d]: %u\n",i,max_n[i]);
  //}
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
void ConnectionMultiApprox::read_callback2() {
  struct evbuffer *input = bufferevent_get_input(bev2);

  Operation *op = NULL;
  bool done, found;

  //initially assume found (for sets that may come through here)
  //is this correct? do we want to assume true in case that 
  //GET was found, but wrong value size (i.e. update value)
  found = true;


  //if (op_queue.size() == 0) V("Spurious read callback.");
  bool full_read = true;
  while (full_read) {
    
      
    int opcode;
    uint32_t opaque;
    full_read = handle_response(this,input, done, found, opcode, opaque, NULL,2);
    if (full_read) {
        if (opcode == CMD_NOOP) {
#ifdef DEBUGMC
            char out[128];
            sprintf(out,"conn l2: %u, reading noop\n",cid);
            write(2,out,strlen(out));
#endif
            continue;
        }
        op = op_queue[2][opaque];
#ifdef DEBUGMC
        char out[128];
        sprintf(out,"conn l2: %u, reading opaque: %u\n",cid,opaque);
        write(2,out,strlen(out));
        output_op(op,2,found);
#endif
        if (strlen(op->key) < 1) {
#ifdef DEBUGMC
            char out2[128];
            sprintf(out2,"conn l2: %u, bad op: %s\n",cid,op->key);
            write(2,out2,strlen(out2));
#endif
            continue;
        }
    } else {
        break;
    }
    

    double now = get_time();
    switch (op->type) {
        case Operation::GET:
            if (done) {
                if ( !found && (options.getset || options.getsetorset) ) {//  &&
                    //(options.twitter_trace != 1)) {
                    int valuelen = op->valuelen;
                    int index = lrand48() % (1024 * 1024);
                    int flags = OP_clu(op) | SRC_L2_M | LOG_OP;
                    //int ret = add_to_copy_keys(string(op->key));
                    //if (ret == 1) {
                        issue_set(op->key, &random_char[index], valuelen, now, flags | ITEM_L1);
                        if (OP_incl(op)) {
                            issue_set(op->key, &random_char[index], valuelen, now, flags | ITEM_L2);
                            last_quiet2 = false; 
                        }
                    //}
                    last_quiet1 = false; 
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
            } else {
                char out[128];
                sprintf(out,"conn l2: %u, not done reading, should do something",cid);
                write(2,out,strlen(out));
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

  }

  double now = get_time();
  if (check_exit_condition(now)) {
      return;
  }

  last_tx = now;
  stats.log_op(op_queue_size[2]);
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
 * Callback called when write requests finish.
 */
void ConnectionMultiApprox::write_callback() {

    //fprintf(stderr,"loaded evbuffer with ops: %u\n",op_queue.size());
}

/**
 * Callback for timer timeouts.
 */
void ConnectionMultiApprox::timer_callback() {
  //fprintf(stderr,"timer up: %d\n",cid);
  drive_write_machine();
}


/* The follow are C trampolines for libevent callbacks. */
void bev_event_cb1_approx(struct bufferevent *bev, short events, void *ptr) {

  ConnectionMultiApprox* conn = (ConnectionMultiApprox*) ptr;
  conn->event_callback1(events);
}

/* The follow are C trampolines for libevent callbacks. */
void bev_event_cb2_approx(struct bufferevent *bev, short events, void *ptr) {

  ConnectionMultiApprox* conn = (ConnectionMultiApprox*) ptr;
  conn->event_callback2(events);
}

void bev_read_cb1_approx(struct bufferevent *bev, void *ptr) {
  ConnectionMultiApprox* conn = (ConnectionMultiApprox*) ptr;
  conn->read_callback1();
}


void bev_read_cb2_approx(struct bufferevent *bev, void *ptr) {
  ConnectionMultiApprox* conn = (ConnectionMultiApprox*) ptr;
  conn->read_callback2();
}

void bev_write_cb_m_approx(struct bufferevent *bev, void *ptr) {
}

void timer_cb_m_approx(evutil_socket_t fd, short what, void *ptr) {
  ConnectionMultiApprox* conn = (ConnectionMultiApprox*) ptr;
  conn->timer_callback();
}

