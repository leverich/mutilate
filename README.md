mutilate
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

