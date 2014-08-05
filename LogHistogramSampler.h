/* -*- c++ -*- */
#ifndef LOGHISTOGRAMSAMPLER_H
#define LOGHISTOGRAMSAMPLER_H

#include <assert.h>
#include <inttypes.h>
#include <math.h>

#include <vector>

#include "mutilate.h"
#include "Operation.h"

#define _POW 1.1

class LogHistogramSampler {
public:
  std::vector<uint64_t> bins;

  std::vector<Operation> samples;

  double sum;
  double sum_sq;

  LogHistogramSampler() = delete;
  LogHistogramSampler(int _bins) : sum(0.0), sum_sq(0.0) {
    assert(_bins > 0);

    bins.resize(_bins + 1, 0);
  }

  void sample(const Operation &op) {
    sample(op.time());
    if (args.save_given) samples.push_back(op);
  }

  void sample(double s) {
    assert(s >= 0);
    size_t bin = log(s)/log(_POW);

    sum += s;
    sum_sq += s*s;

    //    I("%f", sum);

    if ((int64_t) bin < 0) {
      bin = 0;
    } else if (bin >= bins.size()) {
      bin = bins.size() - 1;
    }

    bins[bin]++;
  }

  double average() {
    //    I("%f %d", sum, total());
    return sum / total();
  }

  double stddev() {
    //    I("%f %d", sum, total());
    return sqrt(sum_sq / total() - pow(sum / total(), 2.0));
  }

  double minimum() {
    for (size_t i = 0; i < bins.size(); i++)
      if (bins[i] > 0) return pow(_POW, (double) i + 0.5);
    DIE("Not implemented");
  }

  double get_nth(double nth) {
    uint64_t count = total();
    uint64_t n = 0;
    double target = count * nth/100;

    for (size_t i = 0; i < bins.size(); i++) {
      n += bins[i];

      if (n > target) { // The nth is inside bins[i].
        double left = target - (n - bins[i]);
        return pow(_POW, (double) i) +
          left / bins[i] * (pow(_POW, (double) (i+1)) - pow(_POW, (double) i));
      }
    }

    return pow(_POW, bins.size());
  } 

  uint64_t total() {
    uint64_t sum = 0.0;

    for (auto i: bins) sum += i;

    return sum;
  }

  void accumulate(const LogHistogramSampler &h) {
    assert(bins.size() == h.bins.size());

    for (size_t i = 0; i < bins.size(); i++) bins[i] += h.bins[i];

    sum += h.sum;
    sum_sq += h.sum_sq;

    for (auto i: h.samples) samples.push_back(i);
  }
};

#endif // LOGHISTOGRAMSAMPLER_H
