// -*- c++-mode -*-
#ifndef OPERATION_H
#define OPERATION_H

#include <string>

using namespace std;

class Operation {
public:
  Operation() {
    valuelen = 0;
    opaque = 0;
    flags = 0;
    clsid = 0;
    future = 0;
    curr = 0;
    l1 = NULL;
    type = NOOP;
    appid = 0;
    start_time = 0;
    end_time = 0;
    memset(key,0,256);
  }
  double start_time, end_time;

  enum type_enum {
    GET, SET, DELETE, SASL, NOOP, TOUCH
  };

  type_enum type;
  uint16_t appid;
  uint32_t valuelen;
  uint32_t opaque;
  uint32_t flags;
  uint16_t clsid;
  uint8_t future;
  uint8_t curr;
  char key[256];
  Operation *l1;

  double time() const { return (end_time - start_time) * 1000000; }
};


#endif // OPERATION_H
