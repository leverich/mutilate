/* -*- c++ -*- */

#ifndef ADAPTIVESAMPLER_H
#define ADAPTIVESAMPLER_H

// Simple exponential-backoff adaptive time series sampler.  Will
// record at most max_samples samples out of however many samples are
// thrown at it.  Makes a vague effort to do this evenly over the
// samples given to it.  The sampling is time invariant (i.e. if you
// start inserting samples at a slower rate, they will be
// under-represented).

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <vector>

#include "log.h"

template <class T> class AdaptiveSampler {
public:
  std::vector<T> samples;
  unsigned int sample_rate;
  unsigned int max_samples;
  unsigned int total_samples;

  AdaptiveSampler() = delete;
  AdaptiveSampler(int max) :
    sample_rate(1), max_samples(max), total_samples(0) {
  }

  void sample(T s) {
    total_samples++;

    if (drand48() < (1/(double) sample_rate))
      samples.push_back(s);

    // Throw out half of the samples, double sample_rate.
    if (samples.size() >= max_samples) {
      sample_rate *= 2;

      std::vector<T> half_samples;
      for (unsigned int i = 0; i < samples.size(); i++) {
        if (drand48() > .5) half_samples.push_back(samples[i]);
      }
      samples = half_samples;
    }
  }

  void save_samples(const char* type, const char* filename) {
    FILE *file;

    if ((file = fopen(filename, "a")) == NULL) {
      W("fopen() failed: %s", strerror(errno));
      return;
    }

    for (size_t i = 0; i < samples.size(); i++) {
      fprintf(file, "%s %" PRIu64 " %f\n", type, i, samples[i]);
    }
  }

  double average() {
    double result = 0.0;
    size_t length = samples.size();
    for (size_t i = 0; i < length; i++) result += samples[i];
    return result/length;
  }

  void print_header() {
      printf("#%-6s %6s %8s %8s %8s %8s %8s %8s\n", "type", "size",
         "min", "max", "avg", "90th", "95th", "99th");
  }

  void print_stats(const char *type, const char *size) {
    std::vector<double> samples_copy = samples;
    size_t l = samples_copy.size();

    if (l == 0) {
      printf("%-7s %6s %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", type, size,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
      return;
    }

    sort(samples_copy.begin(), samples_copy.end());

    printf("%-7s %6s %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", type, size,
           samples_copy[0], samples_copy[l-1], average(),
           samples_copy[(l*90)/100], samples_copy[(l*95)/100],
           samples_copy[(l*99)/100]);
  }
};

#endif // ADAPTIVESAMPLER_H
