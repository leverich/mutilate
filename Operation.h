// -*- c++-mode -*-
#ifndef OPERATION_H
#define OPERATION_H

#include <string>

using namespace std;

class Operation {
public:
  double start_time, end_time;

  enum type_enum {
    GET, SET, DELETE, SASL, NOOP, TOUCH
  };

  type_enum type;

  int valuelen;
  uint32_t opaque;
  uint32_t flags;
  uint16_t clsid;
  uint32_t future;
  uint32_t curr;
  string key;
  Operation *l1;

  double time() const { return (end_time - start_time) * 1000000; }
};


#endif // OPERATION_H
