/* -*- c++ -*- */
#ifndef AGENTSTATS_H
#define AGENTSTATS_H

class AgentStats {
public:
  uint64_t rx_bytes, tx_bytes;
  uint64_t gets, sets, accesses, get_misses;
  uint64_t gets_l1, gets_l2, sets_l1, sets_l2;
  uint64_t get_misses_l1, get_misses_l2;
  uint64_t excl_wbs, incl_wbs;
  uint64_t copies_to_l1;
  uint64_t skips;

  double start, stop;
};

#endif // AGENTSTATS_H
