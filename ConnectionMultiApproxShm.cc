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
#include <sys/ipc.h>
#include <sys/shm.h>

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
#include "bipbuffer.h"
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



pthread_mutex_t cid_lock_m_approx_shm = PTHREAD_MUTEX_INITIALIZER;
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

void ConnectionMultiApproxShm::output_op(Operation *op, int type, bool found) {
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
ConnectionMultiApproxShm::ConnectionMultiApproxShm(options_t _options,
                       bool sampling) :
  start_time(0), stats(sampling), options(_options) 
{
  pthread_mutex_lock(&cid_lock_m_approx_shm);
  cid = connids_m++;
  if (cid == 1) {
    init_classes();
    init_inclusives(options.inclusives);
  }
  cid_rate.insert( { cid, 0 } );
  
  pthread_mutex_unlock(&cid_lock_m_approx_shm);
  
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

  read_state  = IDLE;
}


void ConnectionMultiApproxShm::set_queue(queue<Operation*>* a_trace_queue) {
    trace_queue = a_trace_queue;
    trace_queue_n = a_trace_queue->size();
}

void ConnectionMultiApproxShm::set_lock(pthread_mutex_t* a_lock) {
    lock = a_lock;
}

void ConnectionMultiApproxShm::set_g_wbkeys(unordered_map<string,vector<Operation*>> *a_wb_keys) {
    g_wb_keys = a_wb_keys;
}

uint32_t ConnectionMultiApproxShm::get_cid() {
    return cid;
}

int ConnectionMultiApproxShm::add_to_wb_keys(string key) {
    auto pos = wb_keys.find(key);
    if (pos == wb_keys.end()) {
        wb_keys.insert( {key, vector<Operation*>() });
        return 1;
    }
    return 2;
}

int ConnectionMultiApproxShm::add_to_copy_keys(string key) {
    auto pos = copy_keys.find(key);
    if (pos == copy_keys.end()) {
        copy_keys.insert( {key, vector<Operation*>() });
        return 1;
    }
    return 2;
}


void ConnectionMultiApproxShm::del_copy_keys(string key) {

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

int ConnectionMultiApproxShm::add_to_touch_keys(string key) {
    //return touch_keys.assign_if_equal( key, NULL, cid ) != NULL ? 1 : 2;
    auto pos = touch_keys.find(key);
    if (pos == touch_keys.end()) {
        touch_keys.insert( {key, cid });
        return 1;
    }
    return 2;
}


void ConnectionMultiApproxShm::del_touch_keys(string key) {
    //touch_keys.erase(key);
    auto position = touch_keys.find(key);
    if (position != touch_keys.end()) {
        touch_keys.erase(position);
    } else {
        fprintf(stderr,"expected %s, got nuthin\n",key.c_str());
    }
}

int ConnectionMultiApproxShm::issue_op(Operation *Op) {
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
              //issue_noop(1);
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

void ConnectionMultiApproxShm::del_wb_keys(string key) {

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


int ConnectionMultiApproxShm::do_connect() {


  //the client should see for this cid, where the shared memory is
  typedef struct shared_ {
      bipbuf_t bipbuf_in;
      bipbuf_t bipbuf_out;
      pthread_mutex_t lock_in;
      pthread_mutex_t lock_out;
      int shared_id;
  } shared_t;
  
  //this cid gets shared memory
  // ftok to generate unique key
  //char shmkey[64];
  //sprintf(shmkey,"shmfilel1%d",cid);
  int id = cid+100;
  //key_t key = ftok(shmkey,id);
  
  // shmget returns an identifier in shmid
  int shmid = shmget(id,sizeof(shared_t),0666);
  
  // shmat to attach to shared memory
  shared_t* share_l1 = (shared_t*) shmat(shmid,(void*)0,0);

  fprintf(stderr,"cid %d gets shared memory buf l1 %d\n",cid,share_l1->shared_id);
  
  // ftok to generate unique key
  //char shmkey2[64];
  //sprintf(shmkey2,"shmfilel2%d",cid);
  int id2 = cid+200;
  //key_t key2 = ftok(shmkey2,id2);
  
  // shmget returns an identifier in shmid
  int shmid2 = shmget(id2,sizeof(shared_t),0666);
  
  // shmat to attach to shared memory
  shared_t* share_l2 = (shared_t*) shmat(shmid2,(void*)0,0);
  
  fprintf(stderr,"cid %d gets shared memory buf l2 %d\n",cid,share_l2->shared_id);

  //the leads are reveresed (from perspective of server)
  bipbuf_in[1] = &share_l1->bipbuf_out;
  bipbuf_in[2] = &share_l2->bipbuf_out;
  bipbuf_out[1] = &share_l1->bipbuf_in;
  bipbuf_out[2] = &share_l2->bipbuf_in;
  
  lock_in[1] = &share_l1->lock_out;
  lock_in[2] = &share_l2->lock_out;
  lock_out[1] = &share_l1->lock_in;
  lock_out[2] = &share_l2->lock_in;
  read_state  = IDLE;
  return 1;
}

/**
 * Destroy a connection, performing cleanup.
 */
ConnectionMultiApproxShm::~ConnectionMultiApproxShm() {
 

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
void ConnectionMultiApproxShm::reset() {
  // FIXME: Actually check the connection, drain all bufferevents, drain op_q.
  //assert(op_queue.size() == 0);
  //evtimer_del(timer);
  read_state = IDLE;
  write_state = INIT_WRITE;
  stats = ConnectionStats(stats.sampling);
}




/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxShm::offer_get(Operation *pop, int extra) {
  
  uint16_t keylen = strlen(pop->key);
  int level = FLAGS_level(pop->flags);


  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, htons(0),
                        htonl(keylen) };
  //if (quiet) {
  //    h.opcode = CMD_GETQ;
  //}
  h.opaque = htonl(pop->opaque);
 
  int res = 0;
  pthread_mutex_lock(lock_out[level]);
  int gtg = bipbuf_unused(bipbuf_out[level]) > (int)(24+keylen) ? 1 : 0;
  if (gtg) {
     res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)&h,24);
     if (res != 24) {
       fprintf(stderr,"failed offer 24 get level %d\n",level);
       pthread_mutex_unlock(lock_out[level]);
       return 0;
     }
     res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)pop->key,keylen);
     if (res != keylen) {
       fprintf(stderr,"failed offer %d get level %d\n",keylen,level);
       pthread_mutex_unlock(lock_out[level]);
       return 0;
     }
     if (extra == 1) {
         extra_queue.pop();
     }
  } else {
      if (extra == 0) {
        extra_queue.push(pop);
      }
  }
  pthread_mutex_unlock(lock_out[level]);
  return 1;

}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxShm::issue_get_with_len(Operation *pop, double now, bool quiet, uint32_t flags, Operation *l1) {
  
  int level = FLAGS_level(flags);

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
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing get: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,pop->key,pop->valuelen,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }


  offer_get(pop,0);
  stats.tx_bytes += 24 + strlen(pop->key);
  return 1;
}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxShm::issue_get_with_len(const char* key, int valuelen, double now, bool quiet, uint32_t flags, Operation *l1) {

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
  op_queue[level][pop->opaque] = pop;
  op_queue_size[level]++;

#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing get: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,key,valuelen,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  
  offer_get(pop,0);
  stats.tx_bytes += 24 + strlen(pop->key);;
  return 1;
}

/**
 * Issue a get request to the server.
 */
int ConnectionMultiApproxShm::issue_touch(const char* key, int valuelen, double now, int flags) {
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

  int res = 0;
  pthread_mutex_lock(lock_out[level]);
  res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)&h,24);
  if (res != 24) {
    fprintf(stderr,"failed offer 24 touch level %d\n",level);
    pthread_mutex_unlock(lock_out[level]);
    return 0;
  }
  if (res != keylen) {
    bipbuf_offer(bipbuf_out[level],(const unsigned char*)&exp,4);
    fprintf(stderr,"failed offer 4 touch level %d\n",level);
    pthread_mutex_unlock(lock_out[level]);
    return 0;
  }
  res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)pop->key,keylen);
  if (res != keylen) {
    fprintf(stderr,"failed offer %d touch level %d\n",keylen,level);
    pthread_mutex_unlock(lock_out[level]);
    return 0;
  }
  pthread_mutex_unlock(lock_out[level]);
  
  stats.tx_bytes += 24 + keylen;
  
  //stats.log_access(op);
  return 1;
}

/**
 * Issue a delete request to the server.
 */
int ConnectionMultiApproxShm::issue_delete(const char* key, double now, uint32_t flags) {
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
  
  pthread_mutex_lock(lock_out[level]);
  bipbuf_offer(bipbuf_out[level],(const unsigned char*)&h,24);
  bipbuf_offer(bipbuf_out[level],(const unsigned char*)key,keylen);
  pthread_mutex_unlock(lock_out[level]);
  

  stats.tx_bytes += 24 + keylen;
  
  //stats.log_access(op);
  return 1;
}

void ConnectionMultiApproxShm::issue_noop(int level) {
   Operation op;
   

   binary_header_t h = { 0x80, CMD_NOOP, 0x0000,
                         0x00, 0x00, htons(0),
                         0x00 };
   

  //bipbuf_offer(bipbuf[level],&h,24);
}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApproxShm::issue_set(Operation *pop, const char* value, double now, uint32_t flags) {
  
  int level = FLAGS_level(flags);

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
  fprintf(stderr,"cid: %d issing set: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,pop->key,pop->valuelen,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }
  
  offer_set(pop);


  stats.tx_bytes += pop->valuelen + 32 + strlen(pop->key);
  return 1;
}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApproxShm::offer_set(Operation *pop, int extra) {

  uint16_t keylen = strlen(pop->key);
  uint32_t length = pop->valuelen;
  int level = FLAGS_level(pop->flags);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, htons(0),
                        htonl(keylen + 8 + length) }; 
  h.opaque = htonl(pop->opaque);
  
  uint32_t f = htonl(pop->flags);
  uint32_t exp = 0;
  int ret = 0;
  int res = 0;
  pthread_mutex_lock(lock_out[level]);
  int gtg = bipbuf_unused(bipbuf_out[level]) > (int)(32+pop->valuelen) ? 1 : 0;
  if (gtg) {
     res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)&h,24);
     if (res != 24) {
       fprintf(stderr,"failed offer 24 set level %d\n",level);
       pthread_mutex_unlock(lock_out[level]);
       return 0;
     }
     res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)&f,4);
     if (res != 4) {
       fprintf(stderr,"failed offer 4 set level %d\n",level);
       pthread_mutex_unlock(lock_out[level]);
       return 0;
     }
     res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)&exp,4);
     if (res != 4) {
       fprintf(stderr,"failed offer 4 set level %d\n",level);
       pthread_mutex_unlock(lock_out[level]);
       return 0;
     }
     res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)pop->key,keylen);
     if (res != keylen) {
       fprintf(stderr,"failed offer %d set level %d\n",keylen,level);
       pthread_mutex_unlock(lock_out[level]);
       return 0;
     }
     int i = 0;
     int index = lrand48() % (1024 * 1024);
     const char *value = &random_char[index];
     while ((res = bipbuf_offer(bipbuf_out[level],(const unsigned char*)value,length)) != (int)length) {
       pthread_mutex_unlock(lock_out[level]);
       i++;
       if (i > 1000) {
           fprintf(stderr,"failed offer %d set level %d\n",length,level);
           break;
       }
       pthread_mutex_lock(lock_out[level]);
     }
     if (extra == 1) {
         extra_queue.pop();
     }
     ret = 1;
  } else {
      if (extra == 0) {
        extra_queue.push(pop);
      }
      ret = 0;
  }
  pthread_mutex_unlock(lock_out[level]);
  return ret;
}

/**
 * Issue a set request to the server.
 */
int ConnectionMultiApproxShm::issue_set(const char* key, const char* value, int length, double now, uint32_t flags) {
  
  int level = FLAGS_level(flags);
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
  op_queue_size[level]++;
#ifdef DEBUGS
  fprintf(stderr,"cid: %d issing set: %s, size: %u, level %d, flags: %d, opaque: %d\n",cid,key,length,level,flags,pop->opaque);
#endif
  
  if (opaque[level] > OPAQUE_MAX) {
      opaque[level] = 1;
  }

  offer_set(pop);
  stats.tx_bytes += length + 32 + strlen(key);
  return 1;
}

/**
 * Return the oldest live operation in progress.
 */
void ConnectionMultiApproxShm::pop_op(Operation *op) {

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
void ConnectionMultiApproxShm::finish_op(Operation *op, int was_hit) {
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
bool ConnectionMultiApproxShm::check_exit_condition(double now) {
  if (eof && op_queue_size[1] == 0 && op_queue_size[2] == 0) {
      return true;
  }
  if (read_state == INIT_READ) return false;

  return false;
}



/**
 * Request generation loop
 */
void ConnectionMultiApproxShm::drive_write_machine_shm(double now) {

    while (trace_queue->size() > 0) {
        int extra_tries = extra_queue.size();
        for (int i = 0; i < extra_tries; i++) {
            Operation *Op = extra_queue.front(); 
            switch(Op->type)
            {
                case Operation::GET:
                   offer_get(Op,1);
                   break;
                case Operation::SET:
                   offer_set(Op,1);
                   break;
            }
        }

        int nissued = 0;
        int nissuedl2 = 0;
        while (nissued < options.depth && extra_queue.size() == 0) {
            Operation *Op = trace_queue->front(); 
            
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
                return;
            } 
            int gtg = 0;
            pthread_mutex_lock(lock_out[1]);
            switch(Op->type)
            {
                case Operation::GET:
                   gtg = bipbuf_unused(bipbuf_out[1]) > (int)(24+strlen(Op->key)) ? 1 : 0;
                   break;
                case Operation::SET:
                   gtg = bipbuf_unused(bipbuf_out[1]) > (int)(32+Op->valuelen) ? 1 : 0;
                   break;
            }
            pthread_mutex_unlock(lock_out[1]);


            if (gtg) {
                trace_queue->pop();
                int l2issued = issue_op(Op);
                nissuedl2 += l2issued;
                nissued++;
            } else {
                break;
            }
        }

        //wait for response (at least nissued)
        int l2issued = read_response_l1();
        nissuedl2 += l2issued;
        if (nissuedl2 > 0) {
            read_response_l2();
        }
        
    }

}

/**
 * Tries to consume a binary response (in its entirety) from shared memory.
 *
 * @param input evBuffer to read response from
 * @return  true if consumed, false if not enough data in buffer.
 */
static int handle_response(ConnectionMultiApproxShm *conn, unsigned char *input, bool &done, bool &found, int &opcode, uint32_t &opaque, evicted_t *evict, int level) {
  // Read the first 24 bytes as a header
  //int length = evbuffer_get_length(input);
  //if (length < 24) return false;
  //binary_header_t* h =
  //        reinterpret_cast<binary_header_t*>(evbuffer_pullup(input, 24));
  //assert(h);
  binary_header_t* h =
          reinterpret_cast<binary_header_t*>(input);

  uint32_t bl = ntohl(h->body_len);
  uint16_t kl = ntohs(h->key_len);
  uint8_t el = h->extra_len;
  // Not whole response
  int targetLen = 24 + bl;

  opcode = h->opcode;
  opaque = ntohl(h->opaque);
  uint16_t status = ntohs(h->status);
#ifdef DEBUGMC
    fprintf(stderr,"cid: %d handle resp from l%d - opcode: %u opaque: %u keylen: %u extralen: %u datalen: %u status: %u\n",conn->get_cid(),level,
            h->opcode,ntohl(h->opaque),ntohs(h->key_len),h->extra_len,
            ntohl(h->body_len),ntohs(h->status));
#endif

  pthread_mutex_lock(conn->lock_in[level]);
  unsigned char *abuf;
  int tries = 0;
  while ((abuf = bipbuf_poll(conn->bipbuf_in[level],targetLen)) == NULL) {
      pthread_mutex_unlock(conn->lock_in[level]);
      tries++;
      if (tries > 100) {
          //fprintf(stderr,"more than 10000 tries for cid: %d for length %d\n",conn->get_cid(),targetLen);
          return 0;

      }
      pthread_mutex_lock(conn->lock_in[level]);
  }
  unsigned char bbuf[1024*1024];
  unsigned char *buf = (unsigned char*) &bbuf;
  if (abuf != NULL) {
    memcpy(bbuf,abuf,targetLen);
  }
  buf += 24;
  pthread_mutex_unlock(conn->lock_in[level]);


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
      //evbuffer_drain(input, targetLen);

  } else if (opcode == CMD_SET && kl > 0 && evict != NULL) {
    //evbuffer_drain(input,24);
    //unsigned char *buf = evbuffer_pullup(input,bl);
    

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
    fprintf(stderr,"class: %u, serverFlags: %u, evictedFlags: %u\n",evict->clsid,evict->serverFlags,evict->evictedFlags);
  } else if ( (opcode == CMD_TOUCH && status == RESP_NOT_FOUND) || 
              (opcode == CMD_DELETE && status == RESP_NOT_FOUND) ) {
    found = false;
  }

  conn->stats.rx_bytes += targetLen;
  done = true;
  return targetLen;
}

int ConnectionMultiApproxShm::read_response_l1() {
 
    //maybe need mutex etc.
  unsigned char input[64];
  pthread_mutex_lock(lock_in[1]);
  unsigned char *in = bipbuf_peek(bipbuf_in[1],24);
  if (in) {
      memcpy(input,in,24);
  }
  pthread_mutex_unlock(lock_in[1]);
  if (in == NULL) {
      return 0;
  }

  uint32_t responses_expected = op_queue_size[1];
  Operation *op = NULL;
  bool done, found;
  found = true;
  int bytes_read = 1;
  int l2reqs = 0;
  uint32_t responses = 0;
  while (bytes_read > 0 && responses < responses_expected && input) {
    
      
    int opcode;
    uint32_t opaque;
    evicted_t *evict = (evicted_t*)malloc(sizeof(evicted_t));
    memset(evict,0,sizeof(evicted_t));
    bytes_read = handle_response(this,input, done, found, opcode, opaque, evict,1);
    
    if (bytes_read > 0) {
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
        responses++;
    } else {
        if (evict) {
            if (evict->evictedKey) free(evict->evictedKey);
            if (evict->evictedData) free(evict->evictedData);
            free(evict);
        }
        return 0;
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
                l2reqs++;
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
                    l2reqs++;
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
                        l2reqs++;
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
    pthread_mutex_lock(lock_in[1]);
    unsigned char *in = bipbuf_peek(bipbuf_in[1],24);
    //int tries = 0;
    //while (input == NULL) {
    //    tries++;
    //    if (tries > 1000) {
    //        fprintf(stderr,"more than 1000 tries for header cid: %d\n",cid);
    //        break;
    //    }
    //    in = bipbuf_poll(bipbuf_in[1],24);
    //    
    //}
    if (in) {
        memcpy(input,in,24);
        pthread_mutex_unlock(lock_in[1]);
    } else {
        pthread_mutex_unlock(lock_in[1]);
        break; 
    }

  }
  return l2reqs;
}

/**
 * Handle incoming data (responses).
 */
void ConnectionMultiApproxShm::read_response_l2() {

    //maybe need mutex etc.
  unsigned char input[64];
  pthread_mutex_lock(lock_in[2]);
  unsigned char *in = bipbuf_peek(bipbuf_in[2],24);
  if (in) {
      memcpy(input,in,24);
  }
  pthread_mutex_unlock(lock_in[2]);
  if (in == NULL) {
      return;
  }
 
  uint32_t responses_expected = op_queue_size[2];
  Operation *op = NULL;
  bool done, found;
  found = true;
  int bytes_read = 1;
  int l2reqs = 0;
  uint32_t responses = 0;

  while (bytes_read > 0 && responses < responses_expected && input) {
    
    int opcode;
    uint32_t opaque;
    evicted_t *evict = (evicted_t*)malloc(sizeof(evicted_t));
    memset(evict,0,sizeof(evicted_t));
    bytes_read = handle_response(this,input, done, found, opcode, opaque, evict,2);

    if (bytes_read > 0) {
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
        responses++;
    } else {
        return;
    }
    

    double now = get_time();
    switch (op->type) {
        case Operation::GET:
            if ( !found && (options.getset || options.getsetorset) ) { //  &&
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

    pthread_mutex_lock(lock_in[2]);
    unsigned char *in = bipbuf_peek(bipbuf_in[2],24);
    //int tries = 0;
    //while (in == NULL) {
    //    tries++;
    //    if (tries > 2000) {
    //        fprintf(stderr,"more than 2000 tries for header cid: %d\n",cid);
    //        break;
    //    }
    //    in = bipbuf_poll(bipbuf_in[2],24);
    //    
    //}
    if (in) {
        memcpy(input,in,24);
        pthread_mutex_unlock(lock_in[2]);
    } else {
        pthread_mutex_unlock(lock_in[2]);
        break; 
    }

  }
}

