// -*- c++-mode -*-

#ifndef CONNECTIONBINARY_H
#define CONNECTIONBINARY_H

#include "Connection.h"

using namespace std;

class ConnectionBinary : public Connection {
public:
  ConnectionBinary(struct event_base* base, struct evdns_base* evdns,
                   string hostname, string port, options_t options,
                   bool sampling = true) :
    Connection(base, evdns, hostname, port, options, sampling) {};


  ConnectionBinary(struct event_base* base, struct evdns_base* evdns,
                   string hostname, string port, string username,
                   string password, options_t options, bool sampling = true) :
    Connection(base, evdns, hostname, port, username, password, options,
               sampling) {};

  ~ConnectionBinary() {};

  typedef struct {
    uint8_t magic;
    uint8_t opcode;
    uint16_t key_len;
    uint8_t extra_len;
    uint8_t data_type;
    union {
      uint16_t vbucket; // request use
      uint16_t status;  // response use
    };
    uint32_t body_len;
    uint32_t opaque;
    uint64_t version;
  } binary_header_t;

  void issue_get(const char* key, double now = 0.0);
  void issue_set(const char* key, const char* value, int length,
                 double now = 0.0);
  void read_callback();

};

#endif // CONNECTIONBINARY_H
