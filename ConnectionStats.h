/* -*- c++ -*- */
#ifndef CONNECTIONSTATS_H
#define CONNECTIONSTATS_H

#include <algorithm>
#include <inttypes.h>
#include <vector>

#ifdef USE_ADAPTIVE_SAMPLER
#include "AdaptiveSampler.h"
#elif defined(USE_HISTOGRAM_SAMPLER)
#include "HistogramSampler.h"
#else
#include "LogHistogramSampler.h"
#endif
#include "AgentStats.h"
#include "Operation.h"

using namespace std;

class ConnectionStats {
 public:
 ConnectionStats(bool _sampling = true) :
#ifdef USE_ADAPTIVE_SAMPLER
   get_sampler(100000), set_sampler(100000), 
   get_l1_sampler(100000), set_l1_sampler(100000),
   get_l2_sampler(100000), set_l2_sampler(100000),
   access_sampler(100000), op_sampler(100000),
#elif defined(USE_HISTOGRAM_SAMPLER)
   get_sampler(10000,1), set_sampler(10000,1), 
   get_l1_sampler(10000,1), set_l1_sampler(10000,1), 
   get_l2_sampler(10000,1), set_l2_sampler(10000,1), 
   access_sampler(10000,1), op_sampler(1000,1),
#else
   get_sampler(200), set_sampler(200), 
   get_l1_sampler(200), set_l1_sampler(200),
   get_l2_sampler(200), set_l2_sampler(200),
   access_sampler(200), op_sampler(100),
#endif
   rx_bytes(0), tx_bytes(0), 
   gets(0), sets(0), 
   gets_l1(0), sets_l1(0), 
   gets_l2(0), sets_l2(0), 
   accesses(0),
   get_misses(0), 
   get_misses_l1(0), get_misses_l2(0), 
   set_misses_l1(0), set_misses_l2(0), 
   excl_wbs(0), incl_wbs(0), 
   copies_to_l1(0),
   window_gets(0), window_sets(0), window_accesses(0),
   window_get_misses(0), skips(0), sampling(_sampling) {}

#ifdef USE_ADAPTIVE_SAMPLER
  AdaptiveSampler<Operation> get_sampler;
  AdaptiveSampler<Operation> set_sampler;
  AdaptiveSampler<Operation> get_l1_sampler;
  AdaptiveSampler<Operation> set_l1_sampler;
  AdaptiveSampler<Operation> get_l2_sampler;
  AdaptiveSampler<Operation> set_l2_sampler;
  AdaptiveSampler<Operation> access_sampler;
  AdaptiveSampler<double> op_sampler;
#elif defined(USE_HISTOGRAM_SAMPLER)
  HistogramSampler get_sampler;
  HistogramSampler set_sampler;
  HistogramSampler get_l1_sampler;
  HistogramSampler get_l2_sampler;
  HistogramSampler set_l1_sampler;
  HistogramSampler set_l2_sampler;
  HistogramSampler access_sampler;
  HistogramSampler op_sampler;
#else
  LogHistogramSampler get_sampler;
  LogHistogramSampler set_sampler;
  LogHistogramSampler get_l1_sampler;
  LogHistogramSampler set_l1_sampler;
  LogHistogramSampler get_l2_sampler;
  LogHistogramSampler set_l2_sampler;
  LogHistogramSampler access_sampler;
  LogHistogramSampler op_sampler;
#endif

  uint64_t rx_bytes, tx_bytes;
  uint64_t gets, sets; 
  uint64_t gets_l1, sets_l1, gets_l2, sets_l2;
  uint64_t accesses, get_misses;
  uint64_t get_misses_l1, get_misses_l2;
  uint64_t set_misses_l1, set_misses_l2;
  uint64_t excl_wbs, incl_wbs;
  uint64_t copies_to_l1;
  uint64_t window_gets, window_sets,  window_accesses, window_get_misses;
  uint64_t skips;

  double start, stop;

  bool sampling;

  void log_get(Operation& op) { if (sampling) get_sampler.sample(op); window_gets++; gets++; }
  void log_set(Operation& op) { if (sampling) set_sampler.sample(op); window_sets++; sets++; }
  
  void log_get_l1(Operation& op) { if (sampling) get_l1_sampler.sample(op); window_gets++; gets_l1++;  }
  void log_set_l1(Operation& op) { if (sampling) set_l1_sampler.sample(op); window_sets++; sets_l1++;  }
  
  void log_get_l2(Operation& op) { if (sampling) get_l2_sampler.sample(op); window_gets++; gets_l2++;  }
  void log_set_l2(Operation& op) { if (sampling) set_l2_sampler.sample(op); window_sets++; sets_l2++;  }
  void log_access(Operation& op) { //if (sampling) access_sampler.sample(op); 
      window_accesses++; } //accesses++; }
  void log_op (double op)     { if (sampling)  op_sampler.sample(op); }

  double get_qps() {
    return (gets_l1 + gets_l2 + sets_l1 + sets_l2) / (stop - start);
  }

#ifdef USE_ADAPTIVE_SAMPLER
  double get_nth(double nth) {
    vector<double> samples;

    if (samples.size() == 0) return 0;

    for (auto s: get_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: get_l1_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: get_l2_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: set_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: set_l1_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: set_l2_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: access_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);

    sort(samples.begin(), samples.end());

    int l = samples.size();
    int i = (int)((nth * l) / 100);

    assert(i < l);

    return samples[i];
  }
#else
  double get_nth(double nth) {
    // FIXME: nth across gets & sets?
    return get_sampler.get_nth(nth);
  }
#endif

  void accumulate(const ConnectionStats &cs) {
#ifdef USE_ADAPTIVE_SAMPLER
    for (auto i: cs.get_sampler.samples) get_sampler.sample(i); //log_get(i);
    for (auto i: cs.get_l1_sampler.samples) get_l1_sampler.sample(i); //log_get(i);
    for (auto i: cs.get_l2_sampler.samples) get_l2_sampler.sample(i); //log_get(i);
    for (auto i: cs.set_sampler.samples) set_sampler.sample(i); //log_set(i);
    for (auto i: cs.set_l1_sampler.samples) set_l1_sampler.sample(i); //log_set(i);
    for (auto i: cs.set_l2_sampler.samples) set_l2_sampler.sample(i); //log_set(i);
    for (auto i: cs.access_sampler.samples) access_sampler.sample(i); //log_access(i);
    for (auto i: cs.op_sampler.samples)  op_sampler.sample(i); //log_op(i);
#else
    get_sampler.accumulate(cs.get_sampler);
    get_l1_sampler.accumulate(cs.get_l1_sampler);
    get_l2_sampler.accumulate(cs.get_l2_sampler);
    set_sampler.accumulate(cs.set_sampler);
    set_l1_sampler.accumulate(cs.set_l1_sampler);
    set_l2_sampler.accumulate(cs.set_l2_sampler);
    access_sampler.accumulate(cs.access_sampler);
    op_sampler.accumulate(cs.op_sampler);
#endif

    rx_bytes += cs.rx_bytes;
    tx_bytes += cs.tx_bytes;
    gets += cs.gets;
    sets += cs.sets;
    gets_l1 += cs.gets_l1;
    gets_l2 += cs.gets_l2;
    sets_l1 += cs.sets_l1;
    sets_l2 += cs.sets_l2;
    accesses += cs.accesses;
    get_misses += cs.get_misses;
    get_misses_l1 += cs.get_misses_l1;
    get_misses_l2 += cs.get_misses_l2;
    set_misses_l1 += cs.set_misses_l1;
    set_misses_l2 += cs.set_misses_l2;
    excl_wbs += cs.excl_wbs;
    incl_wbs += cs.incl_wbs;
    copies_to_l1 += cs.copies_to_l1;
    skips += cs.skips;

    start = cs.start;
    stop = cs.stop;
  }

  void accumulate(const AgentStats &as) {
    rx_bytes += as.rx_bytes;
    tx_bytes += as.tx_bytes;
    gets += as.gets;
    sets += as.sets;
    gets_l1 += as.gets_l1;
    gets_l2 += as.gets_l2;
    sets_l1 += as.sets_l1;
    sets_l2 += as.sets_l2;
    accesses += as.accesses;
    get_misses += as.get_misses;
    get_misses_l1 += as.get_misses_l1;
    get_misses_l2 += as.get_misses_l2;
    set_misses_l1 += as.set_misses_l1;
    set_misses_l2 += as.set_misses_l2;
    excl_wbs += as.excl_wbs;
    incl_wbs += as.incl_wbs;
    copies_to_l1 += as.copies_to_l1;
    skips += as.skips;

    start = as.start;
    stop = as.stop;
  }

  static void print_header() {
    printf("%-7s %7s %7s %7s %7s %7s %7s %7s %7s %7s %7s\n",
           "#type", "avg", "std", "min", /*"1st",*/ "5th", "10th",
           "50th", "90th", "95th", "99th", "99.9th");
  }

#ifdef USE_ADAPTIVE_SAMPLER
  void print_stats(const char *tag, AdaptiveSampler<Operation> &sampler,
                   bool newline = true) {
    vector<double> copy;

    for (auto i: sampler.samples) copy.push_back(i.time());
    size_t l = copy.size();

    if (l == 0) {
      printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
             tag, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      if (newline) printf("\n");
      return;
    }

    sort(copy.begin(), copy.end());

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, std::accumulate(copy.begin(), copy.end(), 0.0) / l,
           copy[0], copy[(l*1) / 100], copy[(l*5) / 100], copy[(l*10) / 100], copy[(l*50) / 100],
           copy[(l*90) / 100], copy[(l*95) / 100], copy[(l*99) / 100], copy[(l*99.9) / 100]
           );
    if (newline) printf("\n");
  }

  void print_stats(const char *tag, AdaptiveSampler<double> &sampler,
                   bool newline = true) {
    vector<double> copy;

    for (auto i: sampler.samples) copy.push_back(i);
    size_t l = copy.size();

    if (l == 0) { printf("%-7s 0", tag); if (newline) printf("\n"); return; }

    sort(copy.begin(), copy.end());

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, std::accumulate(copy.begin(), copy.end(), 0.0) / l,
           copy[0], copy[(l*1) / 100], copy[(l*5) / 100], copy[(l*10) / 100], copy[(l*50) / 100],
           copy[(l*90) / 100], copy[(l*95) / 100], copy[(l*99) / 100], copy[(l*99.9) / 100]
           );
    if (newline) printf("\n");
  }
#elif defined(USE_HISTOGRAM_SAMPLER)
  void print_stats(const char *tag, HistogramSampler &sampler,
                   bool newline = true) {
    if (sampler.total() == 0) {
      printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
             tag, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      if (newline) printf("\n");
      return;
    }

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, sampler.average(),
           sampler.get_nth(0), sampler.get_nth(1), sampler.get_nth(5),
           sampler.get_nth(10), sampler.get_nth(50), sampler.get_nth(90),
           sampler.get_nth(95), sampler.get_nth(99), sampler.get_nth(99.9));

    if (newline) printf("\n");
  }
#else
  void print_stats(const char *tag, LogHistogramSampler &sampler,
                   bool newline = true) {
    if (sampler.total() == 0) {
      printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
             tag, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      if (newline) printf("\n");
      return;
    }

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, sampler.average(), sampler.stddev(),
           sampler.get_nth(0), sampler.get_nth(5),
           sampler.get_nth(10), sampler.get_nth(50),
           sampler.get_nth(90), sampler.get_nth(95),
           sampler.get_nth(99), sampler.get_nth(99.9) );

    if (newline) printf("\n");
  }
#endif
};

#endif // CONNECTIONSTATS_H
