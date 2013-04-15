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
   get_sampler(100000), set_sampler(100000), op_sampler(100000),
#elif defined(USE_HISTOGRAM_SAMPLER)
   get_sampler(10000,1), set_sampler(10000,1), op_sampler(1000,1),
#else
   get_sampler(200), set_sampler(200), op_sampler(100),
#endif
   rx_bytes(0), tx_bytes(0), gets(0), sets(0),
   get_misses(0), skips(0), sampling(_sampling) {}

#ifdef USE_ADAPTIVE_SAMPLER
  AdaptiveSampler<Operation> get_sampler;
  AdaptiveSampler<Operation> set_sampler;
  AdaptiveSampler<double> op_sampler;
#elif defined(USE_HISTOGRAM_SAMPLER)
  HistogramSampler get_sampler;
  HistogramSampler set_sampler;
  HistogramSampler op_sampler;
#else
  LogHistogramSampler get_sampler;
  LogHistogramSampler set_sampler;
  LogHistogramSampler op_sampler;
#endif

  uint64_t rx_bytes, tx_bytes;
  uint64_t gets, sets, get_misses;
  uint64_t skips;

  double start, stop;

  bool sampling;

  void log_get(Operation& op) { if (sampling) get_sampler.sample(op); gets++; }
  void log_set(Operation& op) { if (sampling) set_sampler.sample(op); sets++; }
  void log_op (double op)     { if (sampling)  op_sampler.sample(op); }

  double get_qps() {
    return (gets + sets) / (stop - start);
  }

#ifdef USE_ADAPTIVE_SAMPLER
  double get_nth(double nth) {
    vector<double> samples;

    if (samples.size() == 0) return 0;

    for (auto s: get_sampler.samples)
      samples.push_back(s.time()); // (s.end_time - s.start_time) * 1000000);
    for (auto s: set_sampler.samples)
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
    for (auto i: cs.set_sampler.samples) set_sampler.sample(i); //log_set(i);
    for (auto i: cs.op_sampler.samples)  op_sampler.sample(i); //log_op(i);
#else
    get_sampler.accumulate(cs.get_sampler);
    set_sampler.accumulate(cs.set_sampler);
    op_sampler.accumulate(cs.op_sampler);
#endif

    rx_bytes += cs.rx_bytes;
    tx_bytes += cs.tx_bytes;
    gets += cs.gets;
    sets += cs.sets;
    get_misses += cs.get_misses;
    skips += cs.skips;

    start = cs.start;
    stop = cs.stop;
  }

  void accumulate(const AgentStats &as) {
    rx_bytes += as.rx_bytes;
    tx_bytes += as.tx_bytes;
    gets += as.gets;
    sets += as.sets;
    get_misses += as.get_misses;
    skips += as.skips;

    start = as.start;
    stop = as.stop;
  }

  static void print_header() {
    printf("%-7s %7s %7s %7s %7s %7s %7s %7s %7s\n",
           "#type", "avg", "std", "min", /*"1st",*/ "5th", "10th",
           "90th", "95th", "99th");
  }

#ifdef USE_ADAPTIVE_SAMPLER
  void print_stats(const char *tag, AdaptiveSampler<Operation> &sampler,
                   bool newline = true) {
    vector<double> copy;

    for (auto i: sampler.samples) copy.push_back(i.time());
    size_t l = copy.size();

    if (l == 0) {
      printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
             tag, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      if (newline) printf("\n");
      return;
    }

    sort(copy.begin(), copy.end());

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, std::accumulate(copy.begin(), copy.end(), 0.0) / l,
           copy[0], copy[(l*1) / 100], copy[(l*5) / 100], copy[(l*10) / 100],
           copy[(l*90) / 100], copy[(l*95) / 100], copy[(l*99) / 100]
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

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, std::accumulate(copy.begin(), copy.end(), 0.0) / l,
           copy[0], copy[(l*1) / 100], copy[(l*5) / 100], copy[(l*10) / 100],
           copy[(l*90) / 100], copy[(l*95) / 100], copy[(l*99) / 100]
           );
    if (newline) printf("\n");
  }
#elif defined(USE_HISTOGRAM_SAMPLER)
  void print_stats(const char *tag, HistogramSampler &sampler,
                   bool newline = true) {
    if (sampler.total() == 0) {
      printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
             tag, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      if (newline) printf("\n");
      return;
    }

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, sampler.average(),
           sampler.get_nth(0), sampler.get_nth(1), sampler.get_nth(5),
           sampler.get_nth(10), sampler.get_nth(90),
           sampler.get_nth(95), sampler.get_nth(99));

    if (newline) printf("\n");
  }
#else
  void print_stats(const char *tag, LogHistogramSampler &sampler,
                   bool newline = true) {
    if (sampler.total() == 0) {
      printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
             tag, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      if (newline) printf("\n");
      return;
    }

    printf("%-7s %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f %7.1f",
           tag, sampler.average(), sampler.stddev(),
           sampler.get_nth(0), /*sampler.get_nth(1),*/ sampler.get_nth(5),
           sampler.get_nth(10), sampler.get_nth(90),
           sampler.get_nth(95), sampler.get_nth(99));

    if (newline) printf("\n");
  }
#endif
};

#endif // CONNECTIONSTATS_H
