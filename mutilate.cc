#include <arpa/inet.h>
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <queue>
#include <string>
#include <vector>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

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

#define MIN(a,b) ((a) < (b) ? (a) : (b))

using namespace std;

gengetopt_args_info args;
char random_char[2 * 1024 * 1024];  // Buffer used to generate random values.

#ifdef HAVE_LIBZMQ
vector<zmq::socket_t*> agent_sockets;
zmq::context_t context(1);
#endif

struct thread_data {
  const vector<string> *servers;
  options_t *options;
  bool master;
#ifdef HAVE_LIBZMQ
  zmq::socket_t *socket;
#endif
};

// struct evdns_base *evdns;

pthread_barrier_t barrier;

double boot_time;

void init_random_stuff();

void go(const vector<string> &servers, options_t &options,
        ConnectionStats &stats
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket = NULL
#endif
);

void do_mutilate(const vector<string> &servers, options_t &options,
                 ConnectionStats &stats, bool master = true
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket = NULL
#endif
);
void args_to_options(options_t* options);
void* thread_main(void *arg);

#ifdef HAVE_LIBZMQ
static std::string s_recv (zmq::socket_t &socket) {
  zmq::message_t message;
  socket.recv(&message);

  return std::string(static_cast<char*>(message.data()), message.size());
}

//  Convert string to 0MQ string and send to socket
static bool s_send (zmq::socket_t &socket, const std::string &string) {
  zmq::message_t message(string.size());
  memcpy(message.data(), string.data(), string.size());

  return socket.send(message);
}

void agent() {
  zmq::context_t context(1);

  zmq::socket_t socket(context, ZMQ_REP);
  socket.bind("tcp://*:5555");

  while (true) {
    zmq::message_t request;

    socket.recv(&request);

    zmq::message_t num(sizeof(int));
    *((int *) num.data()) = args.threads_arg * args.lambda_mul_arg;
    socket.send(num);

    options_t options;
    memcpy(&options, request.data(), sizeof(options));

    vector<string> servers;

    for (int i = 0; i < options.server_given; i++) {
      servers.push_back(s_recv(socket));
      s_send(socket, "ACK");
    }

    for (auto i: servers) {
      V("Got server = %s", i.c_str());
    }

    options.threads = args.threads_arg;

    socket.recv(&request);
    options.lambda_denom = *((int *) request.data());
    s_send(socket, "THANKS");

    //    V("AGENT SLEEPS"); sleep(1);
    options.lambda = (double) options.qps / options.lambda_denom * args.lambda_mul_arg;

    //    if (options.threads > 1)
      pthread_barrier_init(&barrier, NULL, options.threads);

    ConnectionStats stats;

    go(servers, options, stats, &socket);

    AgentStats as;

    as.rx_bytes = stats.rx_bytes;
    as.tx_bytes = stats.tx_bytes;
    as.gets = stats.gets;
    as.sets = stats.sets;
    as.get_misses = stats.get_misses;
    as.start = stats.start;
    as.stop = stats.stop;

    string req = s_recv(socket);
    //    V("req = %s", req.c_str());
    request.rebuild(sizeof(as));
    memcpy(request.data(), &as, sizeof(as));
    socket.send(request);
  }
}

void prep_agent(const vector<string>& servers, options_t& options) {
  int sum = options.lambda_denom;

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
    //    V("Reply: %s", rep.c_str());
    }
  }

  options.lambda_denom = sum;
  options.lambda = (double) options.qps / options.lambda_denom * args.lambda_mul_arg;

  V("lambda_denom = %d", sum);

  for (auto s: agent_sockets) {
    zmq::message_t message(sizeof(sum));
    *((int *) message.data()) = sum;
    s->send(message);
    string rep = s_recv(*s);
  }

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

void sync_agent(zmq::socket_t* socket) {
  //  V("agent: synchronizing");

  if (args.agent_given) {
    for (auto s: agent_sockets) {
      s_send(*s, "sync1");
      string rep = s_recv(*s);
    }
  } else if (args.agentmode_given) {
    string req = s_recv(*socket);
    s_send(*socket, "sync");
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
  //  evthread_use_pthreads();

  //  if ((evdns = evdns_base_new(base, 1)) == 0) DIE("evdns");

#ifdef HAVE_LIBZMQ
  if (args.agentmode_given) {
    agent();
    return 0;
  } else if (args.agent_given) {
    for (unsigned int i = 0; i < args.agent_given; i++) {
      zmq::socket_t *s = new zmq::socket_t(context, ZMQ_REQ);
      string host = string("tcp://") + string(args.agent_arg[i]) + string(":5555");
      s->connect(host.c_str());
      agent_sockets.push_back(s);
    }
  }
#endif

  options_t options;
  args_to_options(&options);

  pthread_barrier_init(&barrier, NULL, options.threads);

  vector<string> servers;
  for (unsigned int s = 0; s < args.server_given; s++)
    servers.push_back(name_to_ipaddr(string(args.server_arg[s])));

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
    int low_qps = 5000;
    double nth;
    int cur_qps;

    go(servers, options, stats);

    nth = stats.get_nth(n);
    peak_qps = stats.get_qps();
    high_qps = stats.get_qps();
    cur_qps = stats.get_qps();

    I("peak qps = %d", high_qps);

    if (nth > x) {

    while ((high_qps > low_qps * 1.02) && cur_qps > 10000) {
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

    while (nth > x && cur_qps > 10000) { // > low_qps) { // 10000) {
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
    stats.print_stats("read",   stats.get_sampler);
    stats.print_stats("update", stats.set_sampler);
    stats.print_stats("op_q",   stats.op_sampler);

    int total = stats.gets + stats.sets;

    printf("\nTotal QPS = %.1f (%d / %.1fs)\n\n",
           total / (stats.stop - stats.start),
           total, stats.stop - stats.start);

    if (args.search_given && peak_qps > 0.0)
      printf("Peak QPS = %.1f\n\n", peak_qps);

    printf("Misses = %" PRIu64 " (%.1f%%)\n\n", stats.get_misses,
           (double) stats.get_misses/stats.gets*100);

    printf("RX %10" PRIu64 " bytes : %6.1f MB/s\n",
           stats.rx_bytes,
           (double) stats.rx_bytes / 1024 / 1024 / (stats.stop - stats.start));
    printf("TX %10" PRIu64 " bytes : %6.1f MB/s\n",
           stats.tx_bytes,
           (double) stats.tx_bytes / 1024 / 1024 / (stats.stop - stats.start));
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
, zmq::socket_t* socket
#endif
) {
#ifdef HAVE_LIBZMQ
  if (args.agent_given > 0) {
    prep_agent(servers, options);
  }
#endif

  if (options.threads > 1) {
    pthread_t pt[options.threads];
    struct thread_data td[options.threads];
    vector<string> ts[options.threads];

    for (int t = 0; t < options.threads; t++) {
      td[t].options = &options;
#ifdef HAVE_LIBZMQ
      td[t].socket = socket;
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

      if (pthread_create(&pt[t], NULL, thread_main, &td[t]))
        DIE("pthread_create() failed");
    }

    for (int t = 0; t < options.threads; t++) {
      ConnectionStats *cs;
      if (pthread_join(pt[t], (void**) &cs)) DIE("pthread_join() failed");
      stats.accumulate(*cs);
      delete cs;
    }
  } else if (options.threads == 1) {
    do_mutilate(servers, options, stats, true
#ifdef HAVE_LIBZMQ
, socket
#endif
);
  } else {
#ifdef HAVE_LIBZMQ
    if (args.agent_given) {
      sync_agent(socket);
      sync_agent(socket);
    }
#endif
  }

#ifdef HAVE_LIBZMQ
  if (args.agent_given > 0) {
    finish_agent(stats);
  }
#endif
}

void* thread_main(void *arg) {
  struct thread_data *td = (struct thread_data *) arg;

  ConnectionStats *cs = new ConnectionStats();

  do_mutilate(*td->servers, *td->options, *cs, td->master
#ifdef HAVE_LIBZMQ
, td->socket
#endif
);

  return cs;
}

void do_mutilate(const vector<string>& servers, options_t& options,
                 ConnectionStats& stats, bool master
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket
#endif
) {
  int loop_flag =
    (options.blocking || args.blocking_given) ? EVLOOP_ONCE : EVLOOP_NONBLOCK;

  char *saveptr = NULL;  // For reentrant strtok().

  struct event_base *base;
  struct evdns_base *evdns;

  if ((base = event_base_new()) == NULL) DIE("event_base_new() fail");
  //  evthread_use_pthreads();

  if ((evdns = evdns_base_new(base, 1)) == 0) DIE("evdns");

  event_base_priority_init(base, 2);

  // FIXME: May want to move this to after all connections established.
  double start = get_time();
  double now = start;

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

    for (int c = 0; c < options.connections; c++) {
      Connection* conn = new Connection(base, evdns, hostname, port, options,
                                        args.agentmode_given ? false :
                                        true);
      connections.push_back(conn);
      if (c == 0) server_lead.push_back(conn);
    }
  }

  // Wait for all Connections to become IDLE.
  while (1) {
    // FIXME: If all connections become ready before event_base_loop
    // is called, this will deadlock.
    event_base_loop(base, EVLOOP_ONCE);

    bool restart = false;
    for (Connection *conn: connections)
      if (conn->read_state != Connection::IDLE)
        restart = true;

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
        if (conn->read_state != Connection::IDLE)
          restart = true;

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
      if (master) sync_agent(socket);
      
      pthread_barrier_wait(&barrier);

      if (master) sync_agent(socket);

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
      conn->drive_write_machine(); // Kick the Connection into motion.
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
      if (conn->read_state != Connection::IDLE)
        restart = true;

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
        if (conn->read_state != Connection::IDLE)
          restart = true;

      if (restart) continue;
      else break;
    }
    }

    //    options.time = old_time;
    for (Connection *conn: connections) {
      conn->reset();
      //      conn->stats = ConnectionStats();
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
    if (master) sync_agent(socket);

    pthread_barrier_wait(&barrier);

    if (master) sync_agent(socket);

    pthread_barrier_wait(&barrier);
    if (master) V("Synchronized.");
  }
#endif

  start = get_time();
  for (Connection *conn: connections) {
    conn->start_time = start;
    conn->drive_write_machine(); // Kick the Connection into motion.
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

  // Tear-down and accumulate stats.
  for (Connection *conn: connections) {
    stats.accumulate(conn->stats);
    delete conn;
  }

  stats.start = start;
  stats.stop = now;

  evdns_base_free(evdns, 0);
  event_base_free(base);
}

void args_to_options(options_t* options) {
  //  bzero(options, sizeof(options_t));
  options->connections = args.connections_arg;
  options->blocking = args.blocking_given;
  options->qps = args.qps_arg;
  options->threads = args.threads_arg;
  options->server_given = args.server_given;
  options->roundrobin = args.roundrobin_given;

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
}

void init_random_stuff() {
  static char lorem[] =
    R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas
turpis dui, suscipit non vehicula non, malesuada id sem. Phasellus
suscipit nisl ut dui consectetur ultrices tincidunt eros
aliquet. Donec feugiat lectus sed nibh ultrices ultrices. Vestibulum
ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia
Curae; Mauris suscipit eros sed justo lobortis at ultrices lacus
molestie. Duis in diam mi. Cum sociis natoque penatibus et magnis dis
parturient montes, nascetur ridiculus mus. Ut cursus viverra
sagittis. Vivamus non facilisis tortor. Integer lectus arcu, sagittis
et eleifend rutrum, condimentum eget sem. Vestibulum tempus tellus non
risus semper semper. Morbi molestie rhoncus mi, in egestas dui
facilisis et.)";

  size_t cursor = 0;

  while (cursor < sizeof(random_char)) {
    size_t max = sizeof(lorem);
    if (sizeof(random_char) - cursor < max)
      max = sizeof(random_char) - cursor;

    memcpy(&random_char[cursor], lorem, max);
    cursor += max;
  }
}

