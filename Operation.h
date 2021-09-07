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

  string key;
  int valuelen;
  uint32_t opaque;
  uint8_t level;

  double time() const { return (end_time - start_time) * 1000000; }
};


#endif // OPERATION_H
