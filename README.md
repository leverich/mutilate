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

