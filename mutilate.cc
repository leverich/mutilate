#include <arpa/inet.h>
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <fcntl.h> /* Added for the nonblocking socket */
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <queue>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
namespace fs = std::filesystem;

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>


#include "common.h" //for zstd
#include "zstd.h" //shippped with mutilate

#include "config.h"

#ifdef HAVE_LIBZMQ
#include <zmq.hpp>
#endif

#include "AdaptiveSampler.h"
#include "AgentStats.h"
#ifndef HAVE_PTHREAD_BARRIER_INIT
#include "barrier.h"
#endif
#include "cmdline.h"
#include "Connection.h"
#include "ConnectionOptions.h"
#include "log.h"
#include "mutilate.h"
#include "util.h"
#include "blockingconcurrentqueue.h"
//#include <folly/concurrency/UnboundedQueue.h>
//#include <folly/concurrency/ConcurrentHashMap.h>

#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define hashsize(n) ((unsigned long int)1<<(n))

using namespace std;
using namespace moodycamel;
//using namespace folly;

int max_n[3] = {0,0,0};
ifstream kvfile;
pthread_mutex_t flock = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t reader_l;
pthread_cond_t reader_ready;
int reader_not_ready = 1;

pthread_mutex_t *item_locks;
int item_lock_hashpower = 14;
        
map<string,int> g_key_hist;

//USPMCQueue<Operation*,true,8,7> g_trace_queue;

//ConcurrentHashMap<int, double> cid_rate;
unordered_map<int,double> cid_rate;
//ConcurrentHashMap<string, vector<Operation*>> copy_keys;
unordered_map<string, vector<Operation*>> copy_keys;
unordered_map<string, vector<Operation*>> wb_keys;
//ConcurrentHashMap<string, vector<Operation*>> touch_keys;
unordered_map<string, int> touch_keys;
//ConcurrentHashMap<string, vector<Operation*>> wb_keys;

gengetopt_args_info args;
char random_char[4 * 1024 * 1024];  // Buffer used to generate random values.

#ifdef HAVE_LIBZMQ
vector<zmq::socket_t*> agent_sockets;
zmq::context_t context(1);
#endif

struct thread_data {
  const vector<string> *servers;
  options_t *options;
  bool master;  // Thread #0, not to be confused with agent master.
#ifdef HAVE_LIBZMQ
  zmq::socket_t *socketz;
#endif
  int id;
  //std::vector<ConcurrentQueue<string>*> trace_queue;
  std::vector<queue<Operation*>*> *trace_queue;
  //std::vector<pthread_mutex_t*> *mutexes;
  pthread_mutex_t* g_lock;
  std::unordered_map<string,vector<Operation*>> *g_wb_keys;
};

struct reader_data {
  //std::vector<ConcurrentQueue<string>*> trace_queue;
  std::vector<queue<Operation*>*> *trace_queue;
  std::vector<pthread_mutex_t*> *mutexes;
  string *trace_filename;
  int twitter_trace;
};

// struct evdns_base *evdns;
    
pthread_t pt[1024];

pthread_barrier_t barrier;

double boot_time;

void init_random_stuff();

void go(const vector<string> &servers, options_t &options,
        ConnectionStats &stats
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socketz = NULL
#endif
);

//void do_mutilate(const vector<string> &servers, options_t &options,
//                 ConnectionStats &stats,std::vector<ConcurrentQueue<string>*> trace_queue,  bool master = true
void do_mutilate(const vector<string> &servers, options_t &options,
                 ConnectionStats &stats,std::vector<queue<Operation*>*> *trace_queue, pthread_mutex_t *g_lock, unordered_map<string,vector<Operation*>> *g_wb_keys,  bool master = true
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socketz = NULL
#endif
);
void args_to_options(options_t* options);
void* thread_main(void *arg);
void* reader_thread(void *arg);

#ifdef HAVE_LIBZMQ
static std::string s_recv (zmq::socket_t &socketz) {
  zmq::message_t message;
  socketz.recv(&message);

  return std::string(static_cast<char*>(message.data()), message.size());
}

//  Convert string to 0MQ string and send to socket
static bool s_send (zmq::socket_t &socketz, const std::string &string) {
  zmq::message_t message(string.size());
  memcpy(message.data(), string.data(), string.size());

  return socketz.send(message);
}

/*
 * Agent protocol
 *
 * PREPARATION PHASE
 *
 * 1. Master -> Agent: options_t
 *
 * options_t contains most of the information needed to drive the
 * client, including the aggregate QPS that has been requested.
 * However, neither the master nor the agent know at this point how
 * many total connections will be made to the memcached server.
 *
 * 2. Agent -> Master: int num = (--threads) * (--lambda_mul)
 *
 * The agent sends a number to the master indicating how many threads
 * this mutilate agent will spawn, and a mutiplier that weights how
 * many QPS this agent's connections will send relative to unweighted
 * connections (i.e. we can request that a purely load-generating
 * agent or an agent on a really fast network connection be more
 * aggressive than other agents or the master).
 *
 * 3. Master -> Agent: lambda_denom
 *
 * The master aggregates all of the numbers collected in (2) and
 * computes a global "lambda_denom".  Which is essentially a count of
 * the total number of Connections across all mutilate instances,
 * weighted by lambda_mul if necessary.  It broadcasts this number to
 * all agents.
 *
 * Each instance of mutilate at this point adjusts the lambda in
 * options_t sent in (1) to account for lambda_denom.  Note that
 * lambda_mul is specific to each instance of mutilate
 * (i.e. --lambda_mul X) and not sent as part of options_t.
 *
 *   lambda = qps / lambda_denom * args.lambda_mul;
 *
 * RUN PHASE
 *
 * After the PREP phase completes, everyone executes do_mutilate().
 * All clients spawn threads, open connections, load the DB, and wait
 * for all connections to become IDLE.  Following that, they
 * synchronize and finally do the heavy lifting.
 * 
 * [IF WARMUP] -1:  Master <-> Agent: Synchronize
 * [IF WARMUP]  0:  Everyone: RUN for options.warmup seconds.
 * 1. Master <-> Agent: Synchronize
 * 2. Everyone: RUN for options.time seconds.
 * 3. Master -> Agent: Dummy message
 * 4. Agent -> Master: Send AgentStats [w/ RX/TX bytes, # gets/sets]
 *
 * The master then aggregates AgentStats across all agents with its
 * own ConnectionStats to compute overall statistics.
 */

void agent() {
  zmq::context_t context(1);

  zmq::socket_t socketz(context, ZMQ_REP);
  if (atoi(args.agent_port_arg) == -1) {
    socketz.bind(string("ipc:///tmp/memcached.sock").c_str());
  } else {
    socketz.bind((string("tcp://*:")+string(args.agent_port_arg)).c_str());
  }

  while (true) {
    zmq::message_t request;

    socketz.recv(&request);

    zmq::message_t num(sizeof(int));
    *((int *) num.data()) = args.threads_arg * args.lambda_mul_arg;
    socketz.send(num);

    options_t options;
    memcpy(&options, request.data(), sizeof(options));

    vector<string> servers;

    for (int i = 0; i < options.server_given; i++) {
      servers.push_back(s_recv(socketz));
      s_send(socketz, "ACK");
    }

    for (auto i: servers) {
      V("Got server = %s", i.c_str());
    }

    options.threads = args.threads_arg;

    socketz.recv(&request);
    options.lambda_denom = *((int *) request.data());
    s_send(socketz, "THANKS");

    //    V("AGENT SLEEPS"); sleep(1);
    options.lambda = (double) options.qps / options.lambda_denom * args.lambda_mul_arg;

    V("lambda_denom = %d, lambda = %f, qps = %d",
      options.lambda_denom, options.lambda, options.qps);

    //    if (options.threads > 1)
      pthread_barrier_init(&barrier, NULL, options.threads);

    ConnectionStats stats;

    go(servers, options, stats, &socketz);

    AgentStats as;

    as.rx_bytes = stats.rx_bytes;
    as.tx_bytes = stats.tx_bytes;
    as.gets = stats.gets;
    as.sets = stats.sets;
    as.get_misses = stats.get_misses;
    as.start = stats.start;
    as.stop = stats.stop;
    as.skips = stats.skips;

    string req = s_recv(socketz);
    //    V("req = %s", req.c_str());
    request.rebuild(sizeof(as));
    memcpy(request.data(), &as, sizeof(as));
    socketz.send(request);
  }
}

void prep_agent(const vector<string>& servers, options_t& options) {
  int sum = options.lambda_denom;
  if (args.measure_connections_given)
    sum = args.measure_connections_arg * options.server_given * options.threads;

  int master_sum = sum;
  if (args.measure_qps_given) {
    sum = 0;
    if (options.qps) options.qps -= args.measure_qps_arg;
  }

  for (auto s: agent_sockets) {
    zmq::message_t message(sizeof(options_t));

    memcpy((void *) message.data(), &options, sizeof(options_t));
    s->send(message);

    zmq::message_t rep;
    s->recv(&rep);
    unsigned int num = *((int *) rep.data());

    sum += options.connections * (options.roundrobin ?
            (servers.size() > num ? servers.size() : num) : 
            (servers.size() * num));

    for (auto i: servers) {
      s_send(*s, i);
      string rep = s_recv(*s);
    }
  }

  // Adjust options_t according to --measure_* arguments.
  options.lambda_denom = sum;
  options.lambda = (double) options.qps / options.lambda_denom *
    args.lambda_mul_arg;

  V("lambda_denom = %d", sum);

  if (args.measure_qps_given) {
    double master_lambda = (double) args.measure_qps_arg / master_sum;

    if (options.qps && master_lambda > options.lambda)
      V("warning: master_lambda (%f) > options.lambda (%f)",
        master_lambda, options.lambda);

    options.lambda = master_lambda;
  }

  if (args.measure_depth_given) options.depth = args.measure_depth_arg;

  for (auto s: agent_sockets) {
    zmq::message_t message(sizeof(sum));
    *((int *) message.data()) = sum;
    s->send(message);
    string rep = s_recv(*s);
  }

  // Master sleeps here to give agents a chance to connect to
  // memcached server before the master, so that the master is never
  // the very first set of connections.  Is this reasonable or
  // necessary?  Most probably not.
  V("MASTER SLEEPS"); sleep_time(1.5);
}

void finish_agent(ConnectionStats &stats) {
  for (auto s: agent_sockets) {
    s_send(*s, "stats");

    AgentStats as;
    zmq::message_t message;

    s->recv(&message);
    memcpy(&as, message.data(), sizeof(as));
    stats.accumulate(as);
  }
}

/*
 * This synchronization routine is ridiculous because the master only
 * has a ZMQ_REQ socket to the agents, but it needs to wait for a
 * message from each agent before it releases them.  In order to get
 * the ZMQ socket into a state where it'll allow the agent to send it
 * a message, it must first send a message ("sync_req").  In order to
 * not leave the socket dangling with an incomplete transaction, the
 * agent must send a reply ("ack").
 *
 * Without this stupid complication it would be:
 *
 * For each agent:
 *   Agent -> Master: sync
 * For each agent:
 *   Master -> Agent: proceed
 *
 * In this way, all agents must arrive at the barrier and the master
 * must receive a message from each of them before it continues.  It
 * then broadcasts the message to proceed, which reasonably limits
 * skew.
 */

void sync_agent(zmq::socket_t* socketz) {
  //  V("agent: synchronizing");

  if (args.agent_given) {
    for (auto s: agent_sockets)
      s_send(*s, "sync_req");

    /* The real sync */
    for (auto s: agent_sockets)
      if (s_recv(*s).compare(string("sync")))
        DIE("sync_agent[M]: out of sync [1]");
    for (auto s: agent_sockets)
      s_send(*s, "proceed");
    /* End sync */

    for (auto s: agent_sockets)
      if (s_recv(*s).compare(string("ack")))
        DIE("sync_agent[M]: out of sync [2]");
  } else if (args.agentmode_given) {
    if (s_recv(*socketz).compare(string("sync_req")))
      DIE("sync_agent[A]: out of sync [1]");

    /* The real sync */
    s_send(*socketz, "sync");
    if (s_recv(*socketz).compare(string("proceed")))
      DIE("sync_agent[A]: out of sync [2]");
    /* End sync */

    s_send(*socketz, "ack");
  }

  //  V("agent: synchronized");
}
#endif

string name_to_ipaddr(string host) {
  char *s_copy = new char[host.length() + 1];
  strcpy(s_copy, host.c_str());

  char *saveptr = NULL;  // For reentrant strtok().

  char *h_ptr = strtok_r(s_copy, ":", &saveptr);
  char *p_ptr = strtok_r(NULL, ":", &saveptr);

  char ipaddr[16];

  if (h_ptr == NULL)
    DIE("strtok(.., \":\") failed to parse %s", host.c_str());

  string hostname = h_ptr;
  string port = "11211";
  if (p_ptr) port = p_ptr;

  struct evutil_addrinfo hints;
  struct evutil_addrinfo *answer = NULL;
  int err;

  /* Build the hints to tell getaddrinfo how to act. */
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC; /* v4 or v6 is fine. */
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP; /* We want a TCP socket */
  /* Only return addresses we can use. */
  hints.ai_flags = EVUTIL_AI_ADDRCONFIG;

  /* Look up the hostname. */
  err = evutil_getaddrinfo(h_ptr, NULL, &hints, &answer);
  if (err < 0) {
    DIE("Error while resolving '%s': %s",
        host.c_str(), evutil_gai_strerror(err));
  }

  if (answer == NULL) DIE("No DNS answer.");

  void *ptr = NULL;
  switch (answer->ai_family) {
  case AF_INET:
    ptr = &((struct sockaddr_in *) answer->ai_addr)->sin_addr;
    break;
  case AF_INET6:
    ptr = &((struct sockaddr_in6 *) answer->ai_addr)->sin6_addr;
    break;
  }

  inet_ntop (answer->ai_family, ptr, ipaddr, 16);

  D("Resolved %s to %s", h_ptr, (string(ipaddr) + ":" + string(port)).c_str());

  delete[] s_copy;

  return string(ipaddr) + ":" + string(port);
}

int main(int argc, char **argv) {
  //event_enable_debug_mode();
  if (cmdline_parser(argc, argv, &args) != 0) exit(-1);

  for (unsigned int i = 0; i < args.verbose_given; i++)
    log_level = (log_level_t) ((int) log_level - 1);

  if (args.quiet_given) log_level = QUIET;

  if (args.depth_arg < 1) DIE("--depth must be >= 1");
  //  if (args.valuesize_arg < 1 || args.valuesize_arg > 1024*1024)
  //    DIE("--valuesize must be >= 1 and <= 1024*1024");
  if (args.qps_arg < 0) DIE("--qps must be >= 0");
  if (args.update_arg < 0.0 || args.update_arg > 1.0)
    DIE("--update must be >= 0.0 and <= 1.0");
  if (args.time_arg < 1) DIE("--time must be >= 1");
  //  if (args.keysize_arg < MINIMUM_KEY_LENGTH)
  //    DIE("--keysize must be >= %d", MINIMUM_KEY_LENGTH);
  if (args.connections_arg < 1 || args.connections_arg > MAXIMUM_CONNECTIONS)
    DIE("--connections must be between [1,%d]", MAXIMUM_CONNECTIONS);
  //  if (get_distribution(args.iadist_arg) == -1)
  //    DIE("--iadist invalid: %s", args.iadist_arg);
  if (!args.server_given && !args.agentmode_given)
    DIE("--server or --agentmode must be specified.");

  // TODO: Discover peers, share arguments.

  init_random_stuff();
  boot_time = get_time();
  setvbuf(stdout, NULL, _IONBF, 0);

  //  struct event_base *base;

  //  if ((base = event_base_new()) == NULL) DIE("event_base_new() fail");
  //evthread_use_pthreads();

  //  if ((evdns = evdns_base_new(base, 1)) == 0) DIE("evdns");

#ifdef HAVE_LIBZMQ
  if (args.agentmode_given) {
    agent();
    return 0;
  } else if (args.agent_given) {
    for (unsigned int i = 0; i < args.agent_given; i++) {
      zmq::socket_t *s = new zmq::socket_t(context, ZMQ_REQ);
      string host = string("tcp://") + string(args.agent_arg[i]) +
        string(":") + string(args.agent_port_arg);
      s->connect(host.c_str());
      agent_sockets.push_back(s);
    }
  }
#endif

  options_t options;
  args_to_options(&options);

  pthread_barrier_init(&barrier, NULL, options.threads);

  vector<string> servers;
  for (unsigned int s = 0; s < args.server_given; s++) {
    if (options.unix_socket) {
        servers.push_back(string(args.server_arg[s]));
    } else {
        servers.push_back(name_to_ipaddr(string(args.server_arg[s])));
    }
  }
  

  ConnectionStats stats;

  double peak_qps = 0.0;

  if (args.search_given) {
    char *n_ptr = strtok(args.search_arg, ":");
    char *x_ptr = strtok(NULL, ":");

    if (n_ptr == NULL || x_ptr == NULL) DIE("Invalid --search argument");

    int n = atoi(n_ptr);
    int x = atoi(x_ptr);

    I("Search-mode.  Find QPS @ %dus %dth percentile.", x, n);

    int high_qps = 2000000;
    int low_qps = 1; // 5000;
    double nth;
    int cur_qps;

    go(servers, options, stats);

    nth = stats.get_nth(n);
    peak_qps = stats.get_qps();
    high_qps = stats.get_qps();
    cur_qps = stats.get_qps();

    I("peak qps = %d, nth = %.1f", high_qps, nth);

    if (nth > x) {
      //    while ((high_qps > low_qps * 1.02) && cur_qps > 10000) {
    while ((high_qps > low_qps * 1.02) && cur_qps > (peak_qps * .1)) {
      cur_qps = (high_qps + low_qps) / 2;

      args_to_options(&options);

      options.qps = cur_qps;
      options.lambda = (double) options.qps / (double) options.lambda_denom * args.lambda_mul_arg;

      stats = ConnectionStats();

      go(servers, options, stats);

      nth = stats.get_nth(n);

      I("cur_qps = %d, get_qps = %f, nth = %f", cur_qps, stats.get_qps(), nth);

      if (nth > x /*|| cur_qps > stats.get_qps() * 1.05*/) high_qps = cur_qps;
      else low_qps = cur_qps;
    }

    //    while (nth > x && cur_qps > 10000) { // > low_qps) { // 10000) {
      //    while (nth > x && cur_qps > 10000 && cur_qps > (low_qps * 0.90)) {
    while (nth > x && cur_qps > (peak_qps * .1) && cur_qps > (low_qps * 0.90)) {
      cur_qps = cur_qps * 98 / 100;

      args_to_options(&options);

      options.qps = cur_qps;
      options.lambda = (double) options.qps / (double) options.lambda_denom * args.lambda_mul_arg;

      stats = ConnectionStats();

      go(servers, options, stats);

      nth = stats.get_nth(n);

      I("cur_qps = %d, get_qps = %f, nth = %f", cur_qps, stats.get_qps(), nth);
    }

    }
  } else if (args.scan_given) {
    char *min_ptr = strtok(args.scan_arg, ":");
    char *max_ptr = strtok(NULL, ":");
    char *step_ptr = strtok(NULL, ":");

    if (min_ptr == NULL || min_ptr == NULL || step_ptr == NULL)
      DIE("Invalid --scan argument");

    int min = atoi(min_ptr);
    int max = atoi(max_ptr);
    int step = atoi(step_ptr);

    printf("%-7s %7s %7s %7s %7s %7s %7s %7s %7s %8s %8s\n",
           "#type", "avg", "min", "1st", "5th", "10th",
           "90th", "95th", "99th", "QPS", "target");

    for (int q = min; q <= max; q += step) {
      args_to_options(&options);

      options.qps = q;
      options.lambda = (double) options.qps / (double) options.lambda_denom * args.lambda_mul_arg;
      //      options.lambda = (double) options.qps / options.connections /
      //        args.server_given /
      //        (args.threads_arg < 1 ? 1 : args.threads_arg);

      stats = ConnectionStats();

      go(servers, options, stats);

      stats.print_stats("read", stats.get_sampler, false);
      printf(" %8.1f", stats.get_qps());
      printf(" %8d\n", q);
    }    
  } else {
    go(servers, options, stats);
  }

  if (!args.scan_given && !args.loadonly_given) {
    stats.print_header();
    stats.print_stats("read     ",   stats.get_sampler);
    stats.print_stats("read_l1  ",   stats.get_l1_sampler);
    stats.print_stats("read_l2  ",   stats.get_l2_sampler);
    stats.print_stats("update_l1", stats.set_l1_sampler);
    stats.print_stats("update_l2", stats.set_l2_sampler);
    stats.print_stats("op_q     ",   stats.op_sampler);

    int total = stats.gets_l1 + stats.gets_l2 + stats.sets_l1 + stats.sets_l2;

    printf("\nTotal QPS = %.1f (%d / %.1fs)\n",
           total / (stats.stop - stats.start),
           total, stats.stop - stats.start);
    
    int rtotal = stats.gets +  stats.sets;
    printf("\nTotal RPS = %.1f (%d / %.1fs)\n",
           rtotal / (stats.stop - stats.start),
           rtotal, stats.stop - stats.start);

    if (args.search_given && peak_qps > 0.0)
      printf("Peak QPS  = %.1f\n", peak_qps);

    printf("\n");

    printf("GET Misses = %" PRIu64 " (%.1f%%)\n", stats.get_misses,
           (double) stats.get_misses/(stats.gets)*100);
    if (servers.size() == 2) {
        int64_t additional = 0;
        if (stats.delete_misses_l2 > 0) {
            additional = stats.delete_misses_l2 - stats.set_excl_hits_l1;
            fprintf(stderr,"delete misses_l2 %lu, delete hits_l2 %lu, excl_set_l1_hits: %lu\n",stats.delete_misses_l2,stats.delete_hits_l2,stats.set_excl_hits_l1);
            if (additional < 0) {
                fprintf(stderr,"additional misses is neg! %ld\n",additional);
                additional = 0;
            }
        }

        for (int i = 0; i < 40; i++) {
            fprintf(stderr,"class %d, gets: %lu, sets: %lu\n",i,stats.gets_cid[i],stats.sets_cid[i]);
        }
        //printf("Misses (L1) = %" PRIu64 " (%.1f%%)\n", stats.get_misses_l1 + stats.set_misses_l1,
        //       (double) (stats.get_misses_l1 + stats.set_misses_l1) /(stats.gets + stats.sets)*100);
        printf("Misses (L1) = %" PRIu64 " (%.1f%%)\n", stats.get_misses_l1 ,
               (double) (stats.get_misses_l1) /(stats.gets)*100);
        printf("SET Misses (L1) = %" PRIu64 " (%.1f%%)\n", stats.set_misses_l1 ,
               (double) (stats.set_misses_l1) /(stats.sets)*100);
        //printf("Misses (L2) = %" PRIu64 " (%.1f%%)\n", stats.get_misses_l2,
        //       (double) (stats.get_misses_l2) /(stats.gets)*100);
        printf("L2 Writes = %" PRIu64 " (%.1f%%)\n", stats.sets_l2,
               (double) stats.sets_l2/(stats.gets+stats.sets)*100);
        
        printf("Incl WBs  = %" PRIu64 " (%.1f%%)\n", stats.incl_wbs,
               (double) stats.incl_wbs/(stats.gets+stats.sets)*100);
        printf("Excl WBs  = %" PRIu64 " (%.1f%%)\n", stats.excl_wbs,
               (double) stats.excl_wbs/(stats.gets+stats.sets)*100);
    }

    printf("Skipped TXs = %" PRIu64 " (%.1f%%)\n\n", stats.skips,
           (double) stats.skips / total * 100);

    printf("RX %10" PRIu64 " bytes : %6.1f MB/s\n",
           stats.rx_bytes,
           (double) stats.rx_bytes / 1024 / 1024 / (stats.stop - stats.start));
    printf("TX %10" PRIu64 " bytes : %6.1f MB/s\n",
           stats.tx_bytes,
           (double) stats.tx_bytes / 1024 / 1024 / (stats.stop - stats.start));

    if (args.save_given) {
      printf("Saving latency samples to %s.\n", args.save_arg);

      FILE *file;
      if ((file = fopen(args.save_arg, "w")) == NULL)
        DIE("--save: failed to open %s: %s", args.save_arg, strerror(errno));

      for (auto i: stats.get_sampler.samples) {
        fprintf(file, "%f %f\n", i.start_time - boot_time, i.time());
      }
    }
  }

  //  if (args.threads_arg > 1) 
    pthread_barrier_destroy(&barrier);

#ifdef HAVE_LIBZMQ
  if (args.agent_given) {
    for (auto i: agent_sockets) delete i;
  }
#endif

  // evdns_base_free(evdns, 0);
  // event_base_free(base);

  cmdline_parser_free(&args);
}

void go(const vector<string>& servers, options_t& options,
        ConnectionStats &stats
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socketz
#endif
) {
#ifdef HAVE_LIBZMQ
  if (args.agent_given > 0) {
    prep_agent(servers, options);
  }
#endif

  //std::vector<ConcurrentQueue<string>*> trace_queue; // = (ConcurrentQueue<string>**)malloc(sizeof(ConcurrentQueue<string>)
  std::vector<queue<Operation*>*> *trace_queue = new std::vector<queue<Operation*>*>(); 
  // = (ConcurrentQueue<string>**)malloc(sizeof(ConcurrentQueue<string>)
  //std::vector<pthread_mutex_t*> *mutexes = new std::vector<pthread_mutex_t*>(); 
  pthread_mutex_t *g_lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)); 
  *g_lock = PTHREAD_MUTEX_INITIALIZER;

  unordered_map<string,vector<Operation*>> *g_wb_keys = new unordered_map<string,vector<Operation*>>();

  for (int i = 0; i <= options.apps; i++) {
  //    //trace_queue.push_back(new ConcurrentQueue<string>(2000000));
  //    pthread_mutex_t *lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
  //    *lock = PTHREAD_MUTEX_INITIALIZER;
  //    mutexes->push_back(lock);
      trace_queue->push_back(new std::queue<Operation*>());
  }
  pthread_mutex_init(&reader_l, NULL);
  pthread_cond_init(&reader_ready, NULL);

  //ConcurrentQueue<string> *trace_queue = new ConcurrentQueue<string>(20000000);
  struct reader_data *rdata = (struct reader_data*)malloc(sizeof(struct reader_data));
  rdata->trace_queue = trace_queue;
  //rdata->mutexes = mutexes;
  rdata->twitter_trace = options.twitter_trace;
  pthread_t rtid;
  if (options.read_file) {
      rdata->trace_filename = new string(options.file_name); 
      int error = 0;
      if ((error = pthread_create(&rtid, NULL,reader_thread,rdata)) != 0) {
        printf("reader thread failed to be created with error code %d\n", error);
      }
      pthread_mutex_lock(&reader_l);
      while (reader_not_ready) 
        pthread_cond_wait(&reader_ready,&reader_l);
      pthread_mutex_unlock(&reader_l);
      
  }

  /* initialize item locks */
  uint32_t item_lock_count = hashsize(item_lock_hashpower);
  item_locks = (pthread_mutex_t*)calloc(item_lock_count, sizeof(pthread_mutex_t));
  for (size_t i = 0; i < item_lock_count; i++) {
      pthread_mutex_init(&item_locks[i], NULL);
  }


  if (options.threads > 1) {
    struct thread_data td[options.threads];
#ifdef __clang__
    vector<string>* ts = static_cast<vector<string>*>(alloca(sizeof(vector<string>) * options.threads));
#else
    vector<string> ts[options.threads];
#endif

#ifdef __linux__
    int current_cpu = -1;
#endif


    for (int t = 0; t < options.threads; t++) {
      td[t].options = &options;
      td[t].id = t;
      td[t].trace_queue = trace_queue;
      td[t].g_lock = g_lock;
      td[t].g_wb_keys = g_wb_keys;
#ifdef HAVE_LIBZMQ
      td[t].socketz = socketz;
#endif
      if (t == 0) td[t].master = true;
      else td[t].master = false;

      if (options.roundrobin) {
        for (unsigned int i = (t % servers.size());
             i < servers.size(); i += options.threads)
          ts[t].push_back(servers[i % servers.size()]);

        td[t].servers = &ts[t];
      } else {
        td[t].servers = &servers;
      }

      pthread_attr_t attr;
      pthread_attr_init(&attr);

#ifdef __linux__
      if (args.affinity_given) {
        int max_cpus = 8 * sizeof(cpu_set_t);
        cpu_set_t m;
        CPU_ZERO(&m);
        sched_getaffinity(0, sizeof(cpu_set_t), &m);

        for (int i = 0; i < max_cpus; i++) {
          int c = (current_cpu + i + 1) % max_cpus;
          if (CPU_ISSET(c, &m)) {
            CPU_ZERO(&m);
            CPU_SET(c, &m);
            int ret;
            if ((ret = pthread_attr_setaffinity_np(&attr,
                                                   sizeof(cpu_set_t), &m)))
              DIE("pthread_attr_setaffinity_np(%d) failed: %s",
                  c, strerror(ret));
            current_cpu = c;
            break;
          }
        }
      }
#endif

      if (pthread_create(&pt[t], &attr, thread_main, &td[t]))
        DIE("pthread_create() failed");
      usleep(t);
    }

    for (int t = 0; t < options.threads; t++) {
      ConnectionStats *cs;
      if (pthread_join(pt[t], (void**) &cs)) DIE("pthread_join() failed");
      stats.accumulate(*cs);
      
      delete cs;
    }
  for (int i = 1; i <= 2; i++) {
      fprintf(stderr,"max issue buf n[%d]: %u\n",i,max_n[i]);
  }
    //delete trace_queue;

  } else if (options.threads == 1) {
    do_mutilate(servers, options, stats, trace_queue, g_lock, g_wb_keys, true
#ifdef HAVE_LIBZMQ
, socketz
#endif
);
  } else {
#ifdef HAVE_LIBZMQ
    if (args.agent_given) {
      sync_agent(socketz);
    }
#endif
  }

#ifdef HAVE_LIBZMQ
  if (args.agent_given > 0) {
    int total = stats.gets + stats.sets;

    V("Local QPS = %.1f (%d / %.1fs)",
      total / (stats.stop - stats.start),
      total, stats.stop - stats.start);    

    finish_agent(stats);
  }
#endif
}

int stick_this_thread_to_core(int core_id) {
   int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= num_cores)
      return EINVAL;

   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();    
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

bool hasEnding (string const &fullString, string const &ending) {
    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}

static char *get_stream(ZSTD_DCtx* dctx, FILE *fin, size_t const buffInSize, void* const buffIn, size_t const buffOutSize, void* const buffOut) {
    /* This loop assumes that the input file is one or more concatenated zstd
     * streams. This example won't work if there is trailing non-zstd data at
     * the end, but streaming decompression in general handles this case.
     * ZSTD_decompressStream() returns 0 exactly when the frame is completed,
     * and doesn't consume input after the frame.
     */
    size_t const toRead = buffInSize;
    size_t read;
    size_t lastRet = 0;
    int isEmpty = 1;
    if ( (read = fread_orDie(buffIn, toRead, fin)) ) {
        isEmpty = 0;
        ZSTD_inBuffer input = { buffIn, read, 0 };
        /* Given a valid frame, zstd won't consume the last byte of the frame
         * until it has flushed all of the decompressed data of the frame.
         * Therefore, instead of checking if the return code is 0, we can
         * decompress just check if input.pos < input.size.
         */
        char *trace = (char*)malloc(buffOutSize*2);
        memset(trace,0,buffOutSize+1);
        size_t tracelen = buffOutSize+1;
        size_t total = 0;
        while (input.pos < input.size) {
            ZSTD_outBuffer output = { buffOut, buffOutSize, 0 };
            /* The return code is zero if the frame is complete, but there may
             * be multiple frames concatenated together. Zstd will automatically
             * reset the context when a frame is complete. Still, calling
             * ZSTD_DCtx_reset() can be useful to reset the context to a clean
             * state, for instance if the last decompression call returned an
             * error.
             */
            
            size_t const ret = ZSTD_decompressStream(dctx, &output , &input);
            
            if (output.pos + total > tracelen) {
                trace = (char*)realloc(trace,(output.pos+total+1));
                tracelen = (output.pos+total+1);
            }
            strncat(trace,(const char*)buffOut,output.pos); 
            total += output.pos;

            lastRet = ret;
        }
        int idx = total;
        while (trace[idx] != '\n') {
            idx--;
        }
        trace[idx] = 0;
        trace[idx+1] = 0;
        return trace;

    }

    if (isEmpty) {
        fprintf(stderr, "input is empty\n");
        return NULL;
    }

    if (lastRet != 0) {
        /* The last return value from ZSTD_decompressStream did not end on a
         * frame, but we reached the end of the file! We assume this is an
         * error, and the input was truncated.
         */
        fprintf(stderr, "EOF before end of stream: %zu\n", lastRet);
        exit(1);
    }
    return NULL;

}

void* reader_thread(void *arg) {
  struct reader_data *rdata = (struct reader_data *) arg;
  //std::vector<ConcurrentQueue<string>*> trace_queue = (std::vector<ConcurrentQueue<string>*>) rdata->trace_queue;
  std::vector<queue<Operation*>*> *trace_queue = (std::vector<queue<Operation*>*>*) rdata->trace_queue;
  //  std::vector<pthread_mutex_t*> *mutexes = (std::vector<pthread_mutex_t*>*) rdata->mutexes;
  int twitter_trace = rdata->twitter_trace;
  string fn = *(rdata->trace_filename);
  srand(time(NULL));
  if (hasEnding(fn,".zst")) {
        string blobfile = fs::path( fn ).filename();
        blobfile.erase(blobfile.length()-4);
        blobfile.insert(0,"/dev/shm/");
        blobfile.append(".data");
        int do_blob = 0;
        int blob = 0;
        if (do_blob) {
            blob = open(blobfile.c_str(),O_CREAT | O_APPEND | O_RDWR, S_IRWXU);
        }
        //init
        const char *filename = fn.c_str();
        FILE* const fin  = fopen_orDie(filename, "rb");
        size_t const buffInSize = ZSTD_DStreamInSize()*1000;
        void*  const buffIn  = malloc_orDie(buffInSize);
        size_t const buffOutSize = ZSTD_DStreamOutSize()*1000;
        void*  const buffOut = malloc_orDie(buffOutSize);

        map<string,Operation*> key_hist;
        ZSTD_DCtx* const dctx = ZSTD_createDCtx();
        //CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");
        //char *leftover = malloc(buffOutSize);
        //memset(leftover,0,buffOutSize);
		//char *trace = (char*)decompress(filename);
        uint64_t nwrites = 0;
        uint64_t nout = 1;
        int batch = 0;
        int cappid = 1;
        fprintf(stderr,"%lu trace queues for connections\n",trace_queue->size());
        char *trace = get_stream(dctx, fin, buffInSize, buffIn, buffOutSize, buffOut);
        while (trace != NULL) {
            char *ftrace = trace;
            char *line = NULL;
            char *line_p = (char*)calloc(2048,sizeof(char));
            while ((line = strsep(&trace,"\n"))) {
                strncpy(line_p,line,2048);
                string full_line(line);
                //check the appid
                int appid = 0;
                int first = 1;
                if (full_line.length() > 10) {
                    
                    if (trace_queue->size() > 0) {
                        stringstream ss(full_line);
                        string rT;
                        string rApp;
                        string rKey;
                        string rOp;
                        string rvaluelen;
                        Operation *Op = new Operation;
                        if (twitter_trace == 1) {
                            string rKeySize;
                            size_t n = std::count(full_line.begin(), full_line.end(), ',');
                            if (n == 6) {
                                getline( ss, rT, ',' );
                                getline( ss, rKey, ',' );
                                getline( ss, rKeySize, ',' );
                                getline( ss, rvaluelen, ',' );
                                getline( ss, rApp, ',' );
                                getline( ss, rOp, ',' );
                                if (rOp.compare("get") == 0) {
                                    Op->type = Operation::GET;
                                } else if (rOp.compare("set") == 0) {
                                    Op->type = Operation::SET;
                                }
                                if (rvaluelen.compare("") == 0 || rvaluelen.size() < 1 || rvaluelen.empty()) {
                                    continue;
                                }
                                appid = cappid;
                                if (nout % 1000 == 0) {
                                    cappid++;
                                    cappid = cappid % trace_queue->size();
                                    if (cappid == 0) cappid = 1;
                                }
                                //appid = stoi(rApp) % trace_queue->size();
                                if (appid == 0) appid = 1;
                                //appid = (rand() % (trace_queue->size()-1)) + 1;
                                //if (appid == 0) appid = 1;
                                
                                
                            } else {
                                continue;
                            }
                            
                        } 
                        else if (twitter_trace == 2) {
                            size_t n = std::count(full_line.begin(), full_line.end(), ',');
                            if (n == 4) {
                                getline( ss, rT, ',');
                                getline( ss, rApp, ',');
                                getline( ss, rOp, ',' );
                                getline( ss, rKey, ',' );
                                getline( ss, rvaluelen, ',' );
                                int ot = stoi(rOp);
                                switch (ot) {
                                    case 1:
                                        Op->type = Operation::GET;
                                        break;
                                    case 2:
                                        Op->type = Operation::SET;
                                        break;
                                }
                                appid = (stoi(rApp)) % trace_queue->size();
                                if (appid == 0) appid = 1;
                                //appid = (nout) % trace_queue->size();
                            } else {
                                continue;
                            }
                        } 
                        else if (twitter_trace == 3) {
                            size_t n = std::count(full_line.begin(), full_line.end(), ',');
                            if (n == 4) {
                                getline( ss, rT, ',');
                                getline( ss, rApp, ',');
                                getline( ss, rOp, ',' );
                                getline( ss, rKey, ',' );
                                getline( ss, rvaluelen, ',' );
                                int ot = stoi(rOp);
                                switch (ot) {
                                    case 1:
                                        Op->type = Operation::GET;
                                        break;
                                    case 2:
                                        Op->type = Operation::SET;
                                        break;
                                }
                                //if (first) {
                                //    appid = (rand() % (trace_queue->size()-1)) + 1;
                                //    if (appid == 0) appid = 1;
                                //    first = 0;
                                //}
                                //batch++;
                                appid = (rand() % (trace_queue->size()-1)) + 1;
                                if (appid == 0) appid = 1;
                            } else {
                                continue;
                            }
                        } 
                        else if (twitter_trace == 4) {
                            size_t n = std::count(full_line.begin(), full_line.end(), ',');
                            if (n == 4) {
                                getline( ss, rT, ',');
                                getline( ss, rKey, ',' );
                                getline( ss, rOp, ',' );
                                getline( ss, rvaluelen, ',' );
                                int ot = stoi(rOp);
                                switch (ot) {
                                    case 1:
                                        Op->type = Operation::GET;
                                        break;
                                    case 2:
                                        Op->type = Operation::SET;
                                        break;
                                }
                                if (rvaluelen == "0") {
                                    rvaluelen = "50000";
                                }

                                appid = (rand() % (trace_queue->size()-1)) + 1;
                                if (appid == 0) appid = 1;
                            } else {
                                continue;
                            }
                        } 
                        int vl = stoi(rvaluelen);
                        if (appid < (int)trace_queue->size() && vl < 524000 && vl > 1) {
                            Op->valuelen = vl;
                            strncpy(Op->key,rKey.c_str(),255);;
                            if (Op->type == Operation::GET) {
                                //find when was last read
                                Operation *last_op = key_hist[rKey];
                                if (last_op != NULL) {
                                    last_op->future = 1; //THE FUTURE IS NOW
                                    Op->curr = 1;
                                    Op->future = 0;
                                    key_hist[rKey] = Op;
                                    g_key_hist[rKey] = 1;
                                } else {
                                    //first ref
                                    Op->curr = 1;
                                    Op->future = 0;
                                    key_hist[rKey] = Op;
                                    g_key_hist[rKey] = 0;
                                }
                            }
                            Op->appid = appid;
                            trace_queue->at(appid)->push(Op);
                            //g_trace_queue.enqueue(Op);
                            //if (twitter_trace == 3) { // && batch == 2) {
                            //    appid = (rand() % (trace_queue->size()-1)) + 1;
                            //    if (appid == 0) appid = 1;
                            //    batch = 0;
                            //}
                        }
                    } else {
                        fprintf(stderr,"big error!\n");
                    }
                }
                //bool res = trace_queue[appid]->try_enqueue(full_line);
                //while (!res) {
                //    //usleep(10);
                //    //res = trace_queue[appid]->try_enqueue(full_line);
                //    nwrites++;
                //}
                nout++;
                if (nout % 1000000 == 0) fprintf(stderr,"decompressed requests: %lu, waits: %lu\n",nout,nwrites);

            }
            free(line_p);
            free(ftrace);
            trace = get_stream(dctx, fin, buffInSize, buffIn, buffOutSize, buffOut);
        }

  	for (int i = 0; i < 10; i++) {
            for (int j = 0; j < (int)trace_queue->size(); j++) {
  	        //trace_queue[j]->enqueue(eof);
                Operation *eof = new Operation;
                eof->type = Operation::SASL;
                eof->appid = j;
  	            trace_queue->at(j)->push(eof);
                //g_trace_queue.enqueue(eof);
                if (i == 0) {
                    fprintf(stderr,"appid %d, tq size: %ld\n",j,trace_queue->at(j)->size());
                }
            }
  	}
        if (do_blob) {
            for (int i = 0; i < (int)trace_queue->size(); i++) {
                queue<Operation*> tmp = *(trace_queue->at(i));
                while (!tmp.empty()) {
                    Operation *Op = tmp.front();
                    int br = write(blob,(void*)(Op),sizeof(Operation));
                    if (br != sizeof(Operation)) {
                        fprintf(stderr,"error writing op!\n");
                    }
                    tmp.pop();
                }

            }
        }

        pthread_mutex_lock(&reader_l);
        if (reader_not_ready) {
            reader_not_ready = 0;
        }
        pthread_mutex_unlock(&reader_l);
        pthread_cond_signal(&reader_ready);
        if (trace) {
            free(trace);
        }
        ZSTD_freeDCtx(dctx);
        fclose_orDie(fin);
        free(buffIn);
        free(buffOut);

	
  } else if (hasEnding(fn,".data")) {
     ifstream trace_file (fn, ios::in | ios::binary);
    uint32_t treqs = 0;
     char *ops = (char*)malloc(sizeof(Operation)*1000000);
     Operation *optr = (Operation*)(ops);
     while (trace_file.good()) {
        trace_file.read((char*)ops,sizeof(Operation)*1000000);
        int tbytes = trace_file.gcount();
        int tops = tbytes/sizeof(Operation);
        for (int i = 0; i < tops; i++) {
            Operation *op = (Operation*)optr;
            string rKey = string(op->key);
            g_key_hist[rKey] = 0;
            if (op->future) {
                g_key_hist[rKey] = 1;
            }
            trace_queue->at(op->appid)->push(op);
            treqs++;
            if (treqs % 1000000 == 0) fprintf(stderr,"loaded requests: %u\n",treqs);
            optr++;

        }
        optr = (Operation*)ops;
     }
     trace_file.close();
     
     pthread_mutex_lock(&reader_l);
     if (reader_not_ready) {
         reader_not_ready = 0;
     }
     pthread_mutex_unlock(&reader_l);
     pthread_cond_signal(&reader_ready);

  }
      //else {
 
  	//ifstream trace_file;
  	//trace_file.open(rdata->trace_filename);
  	//while (trace_file.good()) {
  	//  string line;
  	//  getline(trace_file,line);
  	//  trace_queue->enqueue(line);
  	//}
  	//string eof = "EOF";
  	//for (int i = 0; i < 1000; i++) {
  	//  trace_queue->enqueue(eof);
  	//}
  //}

  return NULL;
}

void* thread_main(void *arg) {
  struct thread_data *td = (struct thread_data *) arg;
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  //int res = stick_this_thread_to_core(td->id % num_cores);
  //if (res != 0) {
  //      DIE("pthread_attr_setaffinity_np(%d) failed: %s",
  //                td->id, strerror(res));
  //}
  ConnectionStats *cs = new ConnectionStats();

  do_mutilate(*td->servers, *td->options, *cs,  td->trace_queue, td->g_lock, td->g_wb_keys, td->master
#ifdef HAVE_LIBZMQ
, td->socketz
#endif
);

  return cs;
}

void do_mutilate(const vector<string>& servers, options_t& options,
                 ConnectionStats& stats, vector<queue<Operation*>*> *trace_queue, pthread_mutex_t* g_lock, unordered_map<string,vector<Operation*>> *g_wb_keys, bool master 
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socketz
#endif
) {
  int loop_flag =
    (options.blocking || args.blocking_given) ? EVLOOP_ONCE : EVLOOP_NONBLOCK;

  char *saveptr = NULL;  // For reentrant strtok().

  struct event_base *base;
  struct evdns_base *evdns;
  struct event_config *config;


  if ((config = event_config_new()) == NULL) DIE("event_config_new() fail");

#ifdef HAVE_DECL_EVENT_BASE_FLAG_PRECISE_TIMER
  if (event_config_set_flag(config, EVENT_BASE_FLAG_PRECISE_TIMER))
        DIE("event_config_set_flag(EVENT_BASE_FLAG_PRECISE_TIMER) fail");
#endif

  if ((base = event_base_new_with_config(config)) == NULL)
    DIE("event_base_new() fail");

  //evthread_use_pthreads();

  if ((evdns = evdns_base_new(base, 1)) == 0) DIE("evdns");

  //  event_base_priority_init(base, 2);

  // FIXME: May want to move this to after all connections established.


  if (servers.size() == 1) {
    vector<Connection*> connections;
    vector<Connection*> server_lead;
    for (auto s: servers) {
      // Split args.server_arg[s] into host:port using strtok().
      char *s_copy = new char[s.length() + 1];
      strcpy(s_copy, s.c_str());

      char *h_ptr = strtok_r(s_copy, ":", &saveptr);
      char *p_ptr = strtok_r(NULL, ":", &saveptr);

      if (h_ptr == NULL) DIE("strtok(.., \":\") failed to parse %s", s.c_str());

      string hostname = h_ptr;
      string port = "11211";
      if (p_ptr) port = p_ptr;

      delete[] s_copy;

      int conns = args.measure_connections_given ? args.measure_connections_arg :
        options.connections;

      srand(time(NULL));
      for (int c = 0; c <= conns; c++) {
        Connection* conn = new Connection(base, evdns, hostname, port, options,
                                          //NULL,//trace_queue,
                                          args.agentmode_given ? false :
                                          true);
        int tries = 120;
        int connected = 0;
        int s = 2;
        for (int i = 0; i < tries; i++) {
          int ret = conn->do_connect();
          if (ret) {
              connected = 1;
              fprintf(stderr,"thread %lu, conn: %d, connected!\n",pthread_self(),c+1);
              break;
          }
          int d = s + rand() % 100;
          //s = s + d;
          
          //fprintf(stderr,"conn: %d, sleeping %d\n",c,d);
          sleep(d);
        } 
        if (connected) {
          //fprintf(stderr,"cid %d gets trace_queue\nfirst: %s",conn->get_cid(),trace_queue->at(conn->get_cid())->front().c_str());
          //conn->set_queue(trace_queue->at(conn->get_cid()));
          //conn->set_lock(mutexes->at(conn->get_cid()));
          connections.push_back(conn);
        } else {
          fprintf(stderr,"conn: %d, not connected!!\n",c);

        }
        if (c == 0) server_lead.push_back(conn);
      }
    }
    double start = get_time();
    double now = start;

    // Wait for all Connections to become IDLE.
    while (1) {
      // FIXME: If all connections become ready before event_base_loop
      // is called, this will deadlock.
      event_base_loop(base, EVLOOP_ONCE);

      bool restart = false;
      for (Connection *conn: connections)
        if (!conn->is_ready()) restart = true;

      if (restart) continue;
      else break;
    }

    // Load database on lead connection for each server.
    if (!options.noload) {
      V("Loading database.");

      for (auto c: server_lead) c->start_loading();

      // Wait for all Connections to become IDLE.
      while (1) {
        // FIXME: If all connections become ready before event_base_loop
        // is called, this will deadlock.
        event_base_loop(base, EVLOOP_ONCE);

        bool restart = false;
        for (Connection *conn: connections)
          if (!conn->is_ready()) restart = true;

        if (restart) continue;
        else break;
      }
    }

    if (options.loadonly) {
      evdns_base_free(evdns, 0);
      event_base_free(base);
      return;
    }

    // FIXME: Remove.  Not needed, testing only.
    //  // FIXME: Synchronize start_time here across threads/nodes.
    //  pthread_barrier_wait(&barrier);

    // Warmup connection.
    if (options.warmup > 0) {
        if (master) V("Warmup start.");

#ifdef HAVE_LIBZMQ
        if (args.agent_given || args.agentmode_given) {
          if (master) V("Synchronizing.");

          // 1. thread barrier: make sure our threads ready before syncing agents
          // 2. sync agents: all threads across all agents are now ready
          // 3. thread barrier: don't release our threads until all agents ready
          pthread_barrier_wait(&barrier);
          if (master) sync_agent(socketz);
          pthread_barrier_wait(&barrier);

          if (master) V("Synchronized.");
        }
#endif

      int old_time = options.time;
      //    options.time = 1;

      start = get_time();
      for (Connection *conn: connections) {
        conn->start_time = start;
        conn->options.time = options.warmup;
        conn->start(); // Kick the Connection into motion.
      }

      while (1) {
        event_base_loop(base, loop_flag);

        //#ifdef USE_CLOCK_GETTIME
        //      now = get_time();
        //#else
        struct timeval now_tv;
        event_base_gettimeofday_cached(base, &now_tv);
        now = tv_to_double(&now_tv);
        //#endif

        bool restart = false;
        for (Connection *conn: connections)
          if (!conn->check_exit_condition(now))
            restart = true;

        if (restart) continue;
        else break;
      }

      bool restart = false;
      for (Connection *conn: connections)
        if (!conn->is_ready()) restart = true;

      if (restart) {

      // Wait for all Connections to become IDLE.
      while (1) {
        // FIXME: If there were to use EVLOOP_ONCE and all connections
        // become ready before event_base_loop is called, this will
        // deadlock.  We should check for IDLE before calling
        // event_base_loop.
        event_base_loop(base, EVLOOP_ONCE); // EVLOOP_NONBLOCK);

        bool restart = false;
        for (Connection *conn: connections)
          if (!conn->is_ready()) restart = true;

        if (restart) continue;
        else break;
      }
      }

      for (Connection *conn: connections) {
        conn->reset();
        conn->options.time = old_time;
      }

      if (master) V("Warmup stop.");
    }


    // FIXME: Synchronize start_time here across threads/nodes.
    pthread_barrier_wait(&barrier);

    if (master && args.wait_given) {
      if (get_time() < boot_time + args.wait_arg) {
        double t = (boot_time + args.wait_arg)-get_time();
        V("Sleeping %.1fs for -W.", t);
        sleep_time(t);
      }
    }

#ifdef HAVE_LIBZMQ
    if (args.agent_given || args.agentmode_given) {
      if (master) V("Synchronizing.");

      pthread_barrier_wait(&barrier);
      if (master) sync_agent(socketz);
      pthread_barrier_wait(&barrier);

      if (master) V("Synchronized.");
    }
#endif

    if (master && !args.scan_given && !args.search_given)
      V("started at %f", get_time());

    start = get_time();
    for (Connection *conn: connections) {
      conn->start_time = start;
      conn->start(); // Kick the Connection into motion.
    }

    //  V("Start = %f", start);

    // Main event loop.
    while (1) {
      event_base_loop(base, loop_flag);

      //#if USE_CLOCK_GETTIME
      //    now = get_time();
      //#else
      struct timeval now_tv;
      event_base_gettimeofday_cached(base, &now_tv);
      now = tv_to_double(&now_tv);
      //#endif

      bool restart = false;
      for (Connection *conn: connections)
        if (!conn->check_exit_condition(now))
          restart = true;

      if (restart) continue;
      else break;
    }

    if (master && !args.scan_given && !args.search_given)
      V("stopped at %f  options.time = %d", get_time(), options.time);

    // Tear-down and accumulate stats.
    for (Connection *conn: connections) {
      stats.accumulate(conn->stats);
      delete conn;
    }

    stats.start = start;
    stats.stop = now;

    event_config_free(config);
    evdns_base_free(evdns, 0);
    event_base_free(base);
  } else if (servers.size() == 2 && ! ( args.approx_given || args.approx_batch_given)) {
    vector<ConnectionMulti*> connections;
    vector<ConnectionMulti*> server_lead;

    string hostname1 = servers[0];
    string hostname2 = servers[1];
    string port = "11211";

    int conns = args.measure_connections_given ? args.measure_connections_arg :
      options.connections;

    srand(time(NULL));
    for (int c = 0; c < conns; c++) {

      int fd1 = -1;

      if ( (fd1 = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket error");
        exit(-1);
      }

      struct sockaddr_un sin1;
      memset(&sin1, 0, sizeof(sin1));
      sin1.sun_family = AF_LOCAL;
      strcpy(sin1.sun_path, hostname1.c_str());

      fcntl(fd1, F_SETFL, O_NONBLOCK); /* Change the socket into non-blocking state   */
      int addrlen;
      addrlen = sizeof(sin1);

      int max_tries = 50;
      int n_tries = 0;
      int s = 10;
      while (connect(fd1, (struct sockaddr*)&sin1, addrlen) == -1) {
        perror("l1 connect error");
        if (n_tries++ > max_tries) {
            fprintf(stderr,"conn l1 %d unable to connect after sleep for %d\n",c+1,s);
            exit(-1);
        }
        int d = s + rand() % 100;
        usleep(d);
        s = (int)((double)s*1.25);
      }
      
      int fd2 = -1;
      if ( (fd2 = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("l2 socket error");
        exit(-1);
      }
      struct sockaddr_un sin2;
      memset(&sin2, 0, sizeof(sin2));
      sin2.sun_family = AF_LOCAL;
      strcpy(sin2.sun_path, hostname2.c_str());
      fcntl(fd2, F_SETFL, O_NONBLOCK); /* Change the socket into non-blocking state   */
      addrlen = sizeof(sin2);
      n_tries = 0;
      s = 10;
      while (connect(fd2, (struct sockaddr*)&sin2, addrlen) == -1) {
        perror("l2 connect error");
        if (n_tries++ > max_tries) {
            fprintf(stderr,"conn l2 %d unable to connect after sleep for %d\n",c+1,s);
            exit(-1);
        }
        int d = s + rand() % 100;
        usleep(d);
        s = (int)((double)s*1.25);
      }


      ConnectionMulti* conn = new ConnectionMulti(base, evdns, 
              hostname1, hostname2, port, options,args.agentmode_given ? false : true, fd1, fd2);
     
      int connected = 0;
      if (conn) {
          connected = 1;
      }
      int cid = conn->get_cid();
      
      if (connected) {
        fprintf(stderr,"cid %d gets l1 fd %d l2 fd %d\n",cid,fd1,fd2);
        fprintf(stderr,"cid %d gets trace_queue\nfirst: %s\n",cid,trace_queue->at(cid)->front()->key);
        if (g_lock != NULL) {
            conn->set_g_wbkeys(g_wb_keys);
            conn->set_lock(g_lock);
        }
        conn->set_queue(trace_queue->at(cid));
        connections.push_back(conn);
      } else {
        fprintf(stderr,"conn multi: %d, not connected!!\n",c);

      }
    }
    
    // wait for all threads to reach here
    pthread_barrier_wait(&barrier);

    fprintf(stderr,"thread %ld gtg\n",pthread_self());
    // Wait for all Connections to become IDLE.
    while (1) {
      // FIXME: If all connections become ready before event_base_loop
      // is called, this will deadlock.
      event_base_loop(base, EVLOOP_ONCE);

      bool restart = false;
      for (ConnectionMulti *conn: connections)
        if (!conn->is_ready()) restart = true;

      if (restart) continue;
      else break;
    }
   
    

    double start = get_time();
    double now = start;
    for (ConnectionMulti *conn: connections) {
        conn->start_time = start;
        conn->start(); // Kick the Connection into motion.
    } 
    //fprintf(stderr,"Start = %f\n", start);

    // Main event loop.
    while (1) {
      event_base_loop(base, loop_flag);
      struct timeval now_tv;
      event_base_gettimeofday_cached(base, &now_tv);
      now = tv_to_double(&now_tv);

      bool restart = false;
      for (ConnectionMulti *conn: connections) {
        if (!conn->check_exit_condition(now)) {
          restart = true;
        }
      }
      if (restart) continue;
      else break;

    }


    //  V("Start = %f", start);

    if (master && !args.scan_given && !args.search_given)
      V("stopped at %f  options.time = %d", get_time(), options.time);

    // Tear-down and accumulate stats.
    for (ConnectionMulti *conn: connections) {
      stats.accumulate(conn->stats);
      delete conn;
    }

    stats.start = start;
    stats.stop = now;

    event_config_free(config);
    evdns_base_free(evdns, 0);
    event_base_free(base);
  } else if (servers.size() == 2 && args.approx_given && !args.approx_batch_given) {
    vector<ConnectionMultiApprox*> connections;
    vector<ConnectionMultiApprox*> server_lead;

    string hostname1 = servers[0];
    string hostname2 = servers[1];
    string port = "11211";

    int conns = args.measure_connections_given ? args.measure_connections_arg :
      options.connections;

    srand(time(NULL));
    for (int c = 0; c < conns; c++) {

      int fd1 = -1;

      if ( (fd1 = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket error");
        exit(-1);
      }

      struct sockaddr_un sin1;
      memset(&sin1, 0, sizeof(sin1));
      sin1.sun_family = AF_LOCAL;
      strcpy(sin1.sun_path, hostname1.c_str());

      fcntl(fd1, F_SETFL, O_NONBLOCK); /* Change the socket into non-blocking state   */
      int addrlen;
      addrlen = sizeof(sin1);

      int max_tries = 50;
      int n_tries = 0;
      int s = 10;
      while (connect(fd1, (struct sockaddr*)&sin1, addrlen) == -1) {
        perror("l1 connect error");
        if (n_tries++ > max_tries) {
            fprintf(stderr,"conn l1 %d unable to connect after sleep for %d\n",c+1,s);
            exit(-1);
        }
        int d = s + rand() % 100;
        usleep(d);
        s = (int)((double)s*1.25);
      }
      
      int fd2 = -1;
      if ( (fd2 = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("l2 socket error");
        exit(-1);
      }
      struct sockaddr_un sin2;
      memset(&sin2, 0, sizeof(sin2));
      sin2.sun_family = AF_LOCAL;
      strcpy(sin2.sun_path, hostname2.c_str());
      fcntl(fd2, F_SETFL, O_NONBLOCK); /* Change the socket into non-blocking state   */
      addrlen = sizeof(sin2);
      n_tries = 0;
      s = 10;
      while (connect(fd2, (struct sockaddr*)&sin2, addrlen) == -1) {
        perror("l2 connect error");
        if (n_tries++ > max_tries) {
            fprintf(stderr,"conn l2 %d unable to connect after sleep for %d\n",c+1,s);
            exit(-1);
        }
        int d = s + rand() % 100;
        usleep(d);
        s = (int)((double)s*1.25);
      }


      ConnectionMultiApprox* conn = new ConnectionMultiApprox(base, evdns, 
              hostname1, hostname2, port, options,args.agentmode_given ? false : true, fd1, fd2);
     
      int connected = 0;
      if (conn) {
          connected = 1;
      }
      int cid = conn->get_cid();
      
      if (connected) {
        fprintf(stderr,"cid %d gets l1 fd %d l2 fd %d\n",cid,fd1,fd2);
        fprintf(stderr,"cid %d gets trace_queue\nfirst: %s\n",cid,trace_queue->at(cid)->front()->key);
        if (g_lock != NULL) {
            conn->set_g_wbkeys(g_wb_keys);
            conn->set_lock(g_lock);
        }
        conn->set_queue(trace_queue->at(cid));
        connections.push_back(conn);
      } else {
        fprintf(stderr,"conn multi: %d, not connected!!\n",c);

      }
    }
    
    // wait for all threads to reach here
    pthread_barrier_wait(&barrier);

    fprintf(stderr,"thread %ld gtg\n",pthread_self());
    // Wait for all Connections to become IDLE.
    while (1) {
      // FIXME: If all connections become ready before event_base_loop
      // is called, this will deadlock.
      event_base_loop(base, EVLOOP_ONCE);

      bool restart = false;
      for (ConnectionMultiApprox *conn: connections)
        if (!conn->is_ready()) restart = true;

      if (restart) continue;
      else break;
    }
   
    

    double start = get_time();
    double now = start;
    for (ConnectionMultiApprox *conn: connections) {
        conn->start_time = start;
        conn->start(); // Kick the Connection into motion.
    } 
    //fprintf(stderr,"Start = %f\n", start);

    // Main event loop.
    while (1) {
      event_base_loop(base, loop_flag);
      struct timeval now_tv;
      event_base_gettimeofday_cached(base, &now_tv);
      now = tv_to_double(&now_tv);

      bool restart = false;
      for (ConnectionMultiApprox *conn: connections) {
        if (!conn->check_exit_condition(now)) {
          restart = true;
        }
      }
      if (restart) continue;
      else break;

    }


    //  V("Start = %f", start);

    if (master && !args.scan_given && !args.search_given)
      V("stopped at %f  options.time = %d", get_time(), options.time);

    // Tear-down and accumulate stats.
    for (ConnectionMultiApprox *conn: connections) {
      stats.accumulate(conn->stats);
      delete conn;
    }

    stats.start = start;
    stats.stop = now;

    event_config_free(config);
    evdns_base_free(evdns, 0);
    event_base_free(base);
  
  } else if (servers.size() == 2 && args.approx_batch_given) {
    vector<ConnectionMultiApproxBatch*> connections;
    vector<ConnectionMultiApproxBatch*> server_lead;

    string hostname1 = servers[0];
    string hostname2 = servers[1];
    string port = "11211";

    int conns = args.measure_connections_given ? args.measure_connections_arg :
      options.connections;

    srand(time(NULL));
    for (int c = 0; c < conns; c++) {

      int fd1 = -1;

      if ( (fd1 = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket error");
        exit(-1);
      }

      struct sockaddr_un sin1;
      memset(&sin1, 0, sizeof(sin1));
      sin1.sun_family = AF_LOCAL;
      strcpy(sin1.sun_path, hostname1.c_str());

      fcntl(fd1, F_SETFL, O_NONBLOCK); /* Change the socket into non-blocking state   */
      int addrlen;
      addrlen = sizeof(sin1);

      int max_tries = 50;
      int n_tries = 0;
      int s = 10;
      while (connect(fd1, (struct sockaddr*)&sin1, addrlen) == -1) {
        perror("l1 connect error");
        if (n_tries++ > max_tries) {
            fprintf(stderr,"conn l1 %d unable to connect after sleep for %d\n",c+1,s);
            exit(-1);
        }
        int d = s + rand() % 100;
        usleep(d);
        s = (int)((double)s*1.25);
      }
      
      int fd2 = -1;
      if ( (fd2 = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("l2 socket error");
        exit(-1);
      }
      struct sockaddr_un sin2;
      memset(&sin2, 0, sizeof(sin2));
      sin2.sun_family = AF_LOCAL;
      strcpy(sin2.sun_path, hostname2.c_str());
      fcntl(fd2, F_SETFL, O_NONBLOCK); /* Change the socket into non-blocking state   */
      addrlen = sizeof(sin2);
      n_tries = 0;
      s = 10;
      while (connect(fd2, (struct sockaddr*)&sin2, addrlen) == -1) {
        perror("l2 connect error");
        if (n_tries++ > max_tries) {
            fprintf(stderr,"conn l2 %d unable to connect after sleep for %d\n",c+1,s);
            exit(-1);
        }
        int d = s + rand() % 100;
        usleep(d);
        s = (int)((double)s*1.25);
      }


      ConnectionMultiApproxBatch* conn = new ConnectionMultiApproxBatch(base, evdns, 
              hostname1, hostname2, port, options,args.agentmode_given ? false : true, fd1, fd2);
     
      int connected = 0;
      if (conn) {
          connected = 1;
      }
      int cid = conn->get_cid();
      
      if (connected) {
        fprintf(stderr,"cid %d gets l1 fd %d l2 fd %d\n",cid,fd1,fd2);
        fprintf(stderr,"cid %d gets trace_queue\nfirst: %s\n",cid,trace_queue->at(cid)->front()->key);
        if (g_lock != NULL) {
            conn->set_g_wbkeys(g_wb_keys);
            conn->set_lock(g_lock);
        }
        conn->set_queue(trace_queue->at(cid));
        connections.push_back(conn);
      } else {
        fprintf(stderr,"conn multi: %d, not connected!!\n",c);

      }
    }
    
    // wait for all threads to reach here
    pthread_barrier_wait(&barrier);

    fprintf(stderr,"thread %ld gtg\n",pthread_self());
    // Wait for all Connections to become IDLE.
    while (1) {
      // FIXME: If all connections become ready before event_base_loop
      // is called, this will deadlock.
      event_base_loop(base, EVLOOP_ONCE);

      bool restart = false;
      for (ConnectionMultiApproxBatch *conn: connections)
        if (!conn->is_ready()) restart = true;

      if (restart) continue;
      else break;
    }
   
    

    double start = get_time();
    double now = start;
    for (ConnectionMultiApproxBatch *conn: connections) {
        conn->start_time = start;
        conn->start(); // Kick the Connection into motion.
    } 
    //fprintf(stderr,"Start = %f\n", start);

    // Main event loop.
    while (1) {
      event_base_loop(base, loop_flag);
      struct timeval now_tv;
      event_base_gettimeofday_cached(base, &now_tv);
      now = tv_to_double(&now_tv);

      bool restart = false;
      for (ConnectionMultiApproxBatch *conn: connections) {
        if (!conn->check_exit_condition(now)) {
          restart = true;
        }
      }
      if (restart) continue;
      else break;

    }


    //  V("Start = %f", start);

    if (master && !args.scan_given && !args.search_given)
      V("stopped at %f  options.time = %d", get_time(), options.time);

    // Tear-down and accumulate stats.
    for (ConnectionMultiApproxBatch *conn: connections) {
      stats.accumulate(conn->stats);
      delete conn;
    }

    stats.start = start;
    stats.stop = now;

    event_config_free(config);
    evdns_base_free(evdns, 0);
    event_base_free(base);
  }
}

void args_to_options(options_t* options) {
  //  bzero(options, sizeof(options_t));
  options->connections = args.connections_arg;
  options->blocking = args.blocking_given;
  options->qps = args.qps_arg;
  options->threads = args.threads_arg;
  options->server_given = args.server_given;
  options->roundrobin = args.roundrobin_given;
  options->apps = args.apps_arg;
  options->rand_admit = args.rand_admit_arg;
  options->threshold = args.threshold_arg;
  options->wb_all = args.wb_all_arg;
  options->ratelimit = args.ratelimit_given;
  if (args.inclusives_given) {
    memset(options->inclusives,0,256);
    strncpy(options->inclusives,args.inclusives_arg,256);
  }

  int connections = options->connections;
  if (options->roundrobin) {
    connections *= (options->server_given > options->threads ?
                    options->server_given : options->threads);
  } else {
    connections *= options->server_given * options->threads;
  }

  //  if (args.agent_given) connections *= (1 + args.agent_given);

  options->lambda_denom = connections > 1 ? connections : 1;
  if (args.lambda_mul_arg > 1) options->lambda_denom *= args.lambda_mul_arg;

  if (options->threads < 1) options->lambda_denom = 0;

  options->lambda = (double) options->qps / (double) options->lambda_denom * args.lambda_mul_arg;

  //  V("%d %d %d %f", options->qps, options->connections,
  //  connections, options->lambda);

  //  if (args.no_record_scale_given)
  //    options->records = args.records_arg;
  //  else
  options->records = args.records_arg / options->server_given;

  options->queries = args.queries_arg / options->server_given;
  
  options->misswindow = args.misswindow_arg;

  options->use_assoc = args.assoc_given;
  options->assoc = args.assoc_arg;
  options->twitter_trace = args.twitter_trace_arg;

  options->unix_socket = args.unix_socket_given;
  options->miss_through = args.miss_through_given;
  options->successful_queries = args.successful_given;
  options->binary = args.binary_given;
  options->redis = args.redis_given;
 
  if (options->use_assoc && !options->redis)
        DIE("assoc must be used with redis");

  options->read_file = args.read_file_given;
  if (args.read_file_given)
    strcpy(options->file_name, args.read_file_arg);

  if (args.prefix_given)
      strcpy(options->prefix,args.prefix_arg);

  //getset mode (first issue get, then set same key if miss)
  options->getset = args.getset_given;
  options->getsetorset = args.getsetorset_given;
  //delete 90 percent of keys after halfway
  //model workload in Rumble and Ousterhout - log structured memory
  //for dram based storage
  options->delete90 = args.delete90_given;

  options->sasl = args.username_given;
  
  if (args.password_given)
    strcpy(options->password, args.password_arg);
  else
    strcpy(options->password, "");

  if (args.username_given)
    strcpy(options->username, args.username_arg);
  else
    strcpy(options->username, "");

  D("options->records = %d", options->records);

  if (!options->records) options->records = 1;
  strcpy(options->keysize, args.keysize_arg);
  //  options->keysize = args.keysize_arg;
  strcpy(options->valuesize, args.valuesize_arg);
  //  options->valuesize = args.valuesize_arg;
  options->update = args.update_arg;
  options->time = args.time_arg;
  options->loadonly = args.loadonly_given;
  options->depth = args.depth_arg;
  options->no_nodelay = args.no_nodelay_given;
  options->noload = args.noload_given;
  options->iadist = get_distribution(args.iadist_arg);
  strcpy(options->ia, args.iadist_arg);
  options->warmup = args.warmup_given ? args.warmup_arg : 0;
  options->oob_thread = false;
  options->skip = args.skip_given;
  options->moderate = args.moderate_given;
}

void init_random_stuff() {
  static char lorem[] =
    R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas turpis dui, suscipit non vehicula non, malesuada id sem. Phasellus suscipit nisl ut dui consectetur ultrices tincidunt eros aliquet. Donec feugiat lectus sed nibh ultrices ultrices. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Mauris suscipit eros sed justo lobortis at ultrices lacus molestie. Duis in diam mi. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Ut cursus viverra sagittis. Vivamus non facilisis tortor. Integer lectus arcu, sagittis et eleifend rutrum, condimentum eget sem. Vestibulum tempus tellus non risus semper semper. Morbi molestie rhoncus mi, in egestas dui facilisis et.)";

  size_t cursor = 0;

  while (cursor < sizeof(random_char)) {
    size_t max = sizeof(lorem);
    if (sizeof(random_char) - cursor < max)
      max = sizeof(random_char) - cursor;

    memcpy(&random_char[cursor], lorem, max);
    cursor += max;
  }
}

