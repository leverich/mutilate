#ifndef DISTRIBUTIONS_H
#define DISTRIBUTIONS_H

// If you change this, make sure to update distributions.cc.
enum distribution_t { UNIFORM, EXPONENTIAL, ZIPFIAN, LATEST };
extern const char* distributions[];

double generate_normal(double mean, double sd);
double generate_poisson(double lambda);
double generate_uniform(double lambda);

distribution_t get_distribution(const char *name);

#endif // DISTRIBUTIONS_H
