/* -*- c++ -*- */
#ifndef HISTOGRAMSAMPLER_H
#define HISTOGRAMSAMPLER_H

#include <inttypes.h>

#include <assert.h>
#include <vector>

#include "Operation.h"

// parameters: # of bins, range? size of bins?

class HistogramSampler {
public:
  std::vector<uint64_t> bins;
  int width;

  double overflow_sum;

  HistogramSampler() = delete;
  HistogramSampler(int _bins, int _width) : overflow_sum(0.0) {
    assert(_bins > 0 && _width > 0);

    bins.resize(_bins + 1, 0);
    width = _width;
  }

  void sample(const Operation &op) {
    sample(op.time());
  }

  void sample(double s) {
    assert(s >= 0);
    size_t bin = s / width;

    if (bin >= bins.size()) {
      bin = bins.size() - 1;
      overflow_sum += s;
    }

    bins[bin]++;
  }

  double average() {
    uint64_t count = total();
    double sum = 0.0;

    for (size_t i = 0; i < bins.size() - 1; i++) {
      sum += bins[i] * (i*width + (i+1)*width) / 2;
    }

    sum += overflow_sum;

    return sum / count;
  }

  double get_nth(double nth) {
    uint64_t count = total();
    uint64_t n = 0;
    double target = count * nth/100;

    for (size_t i = 0; i < bins.size(); i++) {
      n += bins[i];

      if (n > target) { // The nth is inside bins[i].
        double left = target - (n - bins[i]);
        return i*width + left / bins[i] * width;
      }
    }

    return bins.size() * width;
  } 

  uint64_t total() {
    uint64_t sum = 0.0;

    for (auto i: bins) sum += i;

    return sum;
  }

  void accumulate(const HistogramSampler &h) {
    assert(width == h.width && bins.size() == h.bins.size());

    for (size_t i = 0; i < bins.size(); i++) bins[i] += h.bins[i];

    overflow_sum += h.overflow_sum;
  }
};

#endif // HISTOGRAMSAMPLER_H
