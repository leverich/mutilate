// -*- c++-mode -*-

#ifndef CONNECTIONASCII_H
#define CONNECTIONASCII_H

#include "Connection.h"

using namespace std;

class ConnectionAscii : public Connection {
public:
  ConnectionAscii(struct event_base* base, struct evdns_base* evdns,
                  string hostname, string port, options_t options,
                  bool sampling = true) :
    Connection(base, evdns, hostname, port, options, sampling) {};


  ConnectionAscii(struct event_base* base, struct evdns_base* evdns,
                  string hostname, string port, string username,
                  string password, options_t options, bool sampling = true) :
    Connection(base, evdns, hostname, port, username, password, options,
               sampling) {};

  ~ConnectionAscii() {};

  void issue_get(const char* key, double now = 0.0);
  void issue_set(const char* key, const char* value, int length,
                 double now = 0.0);
  void read_callback();
};

#endif // CONNECTIONASCII_H

