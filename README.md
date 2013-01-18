Mutilate
========

Mutilate is a memcached load generator designed for high request
rates, good tail-latency measurements, and realistic request stream
generation.

Requirements
============

1. A C++0x compiler
2. scons
3. libevent
4. gengetopt
5. zeromq (optional)

Mutilate has only been thoroughly tested on Ubuntu 11.10.  We'll flesh
out compatibility over time.

Building
========

    apt-get install scons libevent-dev gengetopt libzmq-dev
    scons

Usage
=====

Type './mutilate -h' for a full list of command-line options.  At
minimum, a server must be specified.

    $ ./mutilate -s localhost
    #type       avg     min     1st     5th    10th    90th    95th    99th
    read       52.4    41.0    43.1    45.2    48.1    55.8    56.6    71.5
    update      0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0
    op_q        1.5     1.0     1.0     1.1     1.1     1.9     2.0     2.0
    
    Total QPS = 18416.6 (92083 / 5.0s)
    
    Misses = 0 (0.0%)
    
    RX   22744501 bytes :    4.3 MB/s
    TX    3315024 bytes :    0.6 MB/s

Mutilate reports the latency (average, minimum, and various
percentiles) for get and set commands, as well as achieved QPS and
network goodput.

To achieve high request rate, you must configure mutilate to use
multiple threads, multiple connections, connection pipelining, or
remote agents.

    $ ./mutilate -s zephyr2-10g -T 24 -c 8
    #type       avg     min     1st     5th    10th    90th    95th    99th
    read      598.8    86.0   437.2   466.6   482.6   977.0  1075.8  1170.6
    update      0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0
    op_q        1.5     1.0     1.0     1.1     1.1     1.9     1.9     2.0
    
    Total QPS = 318710.8 (1593559 / 5.0s)
    
    Misses = 0 (0.0%)
    
    RX  393609073 bytes :   75.1 MB/s
    TX   57374136 bytes :   10.9 MB/s

Command-line Options
====================
Usage: mutilate -s server[:port] [options]

"High-performance" memcached benchmarking tool

  -h, --help               Print help and exit
      --version            Print version and exit
  -v, --verbose            Verbosity. Repeat for more verbose.
      --quiet              Disable log messages.

Basic options:
  -s, --server=STRING      Memcached server hostname[:port].  Repeat to specify
                             multiple servers.
  -q, --qps=INT            Target aggregate QPS. 0 = peak QPS.  (default=`0')
  -t, --time=INT           Maximum time to run (seconds).  (default=`5')
  -K, --keysize=STRING     Length of memcached keys (distribution).
                             (default=`30')
  -V, --valuesize=STRING   Length of memcached values (distribution).
                             (default=`200')
  -r, --records=INT        Number of memcached records to use.  If multiple
                             memcached servers are given, this number is
                             divided by the number of servers.
                             (default=`10000')
  -u, --update=FLOAT       Ratio of set:get commands.  (default=`0.0')

Advanced options:
  -T, --threads=INT        Number of threads to spawn.  (default=`1')
  -c, --connections=INT    Connections to establish per server.  (default=`1')
  -d, --depth=INT          Maximum depth to pipeline requests.  (default=`1')
  -R, --roundrobin         Assign threads to servers in round-robin fashion.
                             By default, each thread connects to every server.
  -i, --iadist=STRING      Inter-arrival distribution (distribution).  Note:
                             The distribution will automatically be adjusted to
                             match the QPS given by --qps.
                             (default=`exponential')
      --noload             Skip database loading.
      --loadonly           Load database and then exit.
  -B, --blocking           Use blocking epoll().  May increase latency.
  -D, --no_nodelay         Don't use TCP_NODELAY.
  -w, --warmup=INT         Warmup time before starting measurement.
  -W, --wait=INT           Time to wait after startup to start measurement.
  -S, --search=N:X         Search for the QPS where N-order statistic < Xus.
                             (i.e. --search 95:1000 means find the QPS where
                             95% of requests are faster than 1000us).
      --scan=min:max:step  Scan latency across QPS rates from min to max.

Agent-mode options:
  -A, --agentmode          Run client in agent mode.
  -a, --agent=host         Enlist remote agent.
  -l, --lambda_mul=INT     Lambda multiplier.  Increases share of QPS for this
                             client.  (default=`1')

Some options take a 'distribution' as an argument.
Distributions are specified by <distribution>[:<param1>[,...]].
Parameters are not required.  The following distributions are supported:

   [fixed:]<value>              Always generates <value>.
   uniform:<max>                Uniform distribution between 0 and <max>.
   normal:<mean>,<sd>           Normal distribution.
   exponential:<lambda>         Exponential distribution.
   pareto:<loc>,<scale>,<shape> Generalized Pareto distribution.
   gev:<loc>,<scale>,<shape>    Generalized Extreme Value distribution.

   To recreate the Facebook "ETC" request stream from [1], the
   following hard-coded distributions are also provided:

   fb_value   = a hard-coded discrete and GPareto PDF of value sizes
   fb_key     = "gev:30.7984,8.20449,0.078688", key-size distribution
   fb_ia      = "pareto:0.0,16.0292,0.154971", inter-arrival time dist.

[1] Berk Atikoglu et al., Workload Analysis of a Large-Scale Key-Value Store,
    SIGMETRICS 2012
