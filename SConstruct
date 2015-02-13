#!/usr/bin/python
import os
import sys

env = Environment(ENV = os.environ)

env['HAVE_POSIX_BARRIER'] = True

env.Append(CPPPATH = ['/usr/local/include', '/opt/local/include'])
env.Append(LIBPATH = ['/opt/local/lib'])
env.Append(CCFLAGS = '-std=c++11 -D_GNU_SOURCE')
if sys.platform == 'darwin':
    env['CC']  = 'clang'
    env['CXX'] = 'clang++'

conf = env.Configure(config_h = "config.h")
conf.Define("__STDC_FORMAT_MACROS")
if not conf.CheckCXX():
    print "A compiler with C++11 support is required."
    Exit(1)
print "Checking for gengetopt...",
if env.Execute("@which gengetopt &> /dev/null"):
    print "not found (required)"
    Exit(1)
else: print "found"
if not conf.CheckLibWithHeader("event", "event2/event.h", "C++"):
    print "libevent required"
    Exit(1)
conf.CheckDeclaration("EVENT_BASE_FLAG_PRECISE_TIMER", '#include <event2/event.h>', "C++")
if not conf.CheckLibWithHeader("pthread", "pthread.h", "C++"):
    print "pthread required"
    Exit(1)
conf.CheckLib("rt", "clock_gettime", language="C++")
conf.CheckLibWithHeader("zmq", "zmq.hpp", "C++")
if not conf.CheckFunc('pthread_barrier_init'):
    conf.env['HAVE_POSIX_BARRIER'] = False

env = conf.Finish()

env.Append(CFLAGS = ' -O3 -Wall -g')
env.Append(CPPFLAGS = ' -O3 -Wall -g')

env.Command(['cmdline.cc', 'cmdline.h'], 'cmdline.ggo', 'gengetopt < $SOURCE')

src = Split("""mutilate.cc cmdline.cc log.cc distributions.cc util.cc
               Connection.cc Protocol.cc Generator.cc""")

if not env['HAVE_POSIX_BARRIER']: # USE_POSIX_BARRIER:
    src += ['barrier.cc']

env.Program(target='mutilate', source=src)
env.Program(target='gtest', source=['TestGenerator.cc', 'log.cc', 'util.cc',
                                    'Generator.cc'])
