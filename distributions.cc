#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "distributions.h"
#include "log.h"

const char* distributions[] =
  { "uniform", "exponential", "zipfian", "latest", NULL };

distribution_t get_distribution(const char *name) {
  for (int i = 0; distributions[i] != NULL; i++)
    if (!strcmp(distributions[i], name))
      return (distribution_t) i;
  return (distribution_t) -1;
}

double generate_normal(double mean, double sd) {
  double U = drand48();
  double V = drand48();
  double N = sqrt(-2 * log(U)) * cos(2 * M_PI * V);
  return mean + sd * N;
}

double generate_poisson(double lambda) {
  if (lambda <= 0.0) return 0;
  double U = drand48();
  return -log(U)/lambda;
}

double generate_uniform(double lambda) {
  if (lambda <= 0.0) return 0;
  return 1.0 / lambda;
}
