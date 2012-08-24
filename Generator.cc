// -*- c++ -*-

#include "config.h"

#include "Generator.h"

Generator* createFacebookKey() { return new GEV(30.7984, 8.20449, 0.078688); }

Generator* createFacebookValue() {
  Generator* g = new GPareto(15.0, 214.476, 0.348238);

  Discrete* d = new Discrete(g);
  d->add(0.00536, 0.0);
  d->add(0.00047, 1.0);
  d->add(0.17820, 2.0);
  d->add(0.09239, 3.0);
  d->add(0.00018, 4.0);
  d->add(0.02740, 5.0);
  d->add(0.00065, 6.0);
  d->add(0.00606, 7.0);
  d->add(0.00023, 8.0);
  d->add(0.00837, 9.0);
  d->add(0.00837, 10.0);
  d->add(0.08989, 11.0);
  d->add(0.00092, 12.0);
  d->add(0.00326, 13.0);
  d->add(0.01980, 14.0);

  return d;
}

Generator* createFacebookIA() { return new GPareto(0, 16.0292, 0.154971); }

Generator* createGenerator(std::string str) {
  if (!strcmp(str.c_str(), "fb_key")) return createFacebookKey();
  else if (!strcmp(str.c_str(), "fb_value")) return createFacebookValue();
  else if (!strcmp(str.c_str(), "fb_ia")) return createFacebookIA();

  char *s_copy = new char[str.length() + 1];
  strcpy(s_copy, str.c_str());
  char *saveptr = NULL;

  if (atoi(s_copy) != 0 || !strcmp(s_copy, "0")) {
    double v = atof(s_copy);
    delete[] s_copy;
    return new Fixed(v);
  }

  char *t_ptr = strtok_r(s_copy, ":", &saveptr);
  char *a_ptr = strtok_r(NULL, ":", &saveptr);

  if (t_ptr == NULL) // || a_ptr == NULL)
    DIE("strtok(.., \":\") failed to parse %s", str.c_str());

  char t = t_ptr[0];

  saveptr = NULL;
  char *s1 = strtok_r(a_ptr, ",", &saveptr);
  char *s2 = strtok_r(NULL, ",", &saveptr);
  char *s3 = strtok_r(NULL, ",", &saveptr);

  double a1 = s1 ? atof(s1) : 0.0;
  double a2 = s2 ? atof(s2) : 0.0;
  double a3 = s3 ? atof(s3) : 0.0;

  delete[] s_copy;

  if      (strcasestr(str.c_str(), "fixed")) return new Fixed(a1);
  else if (strcasestr(str.c_str(), "normal")) return new Normal(a1, a2);
  else if (strcasestr(str.c_str(), "exponential")) return new Exponential(a1);
  else if (strcasestr(str.c_str(), "pareto")) return new GPareto(a1, a2, a3);
  else if (strcasestr(str.c_str(), "gev")) return new GEV(a1, a2, a3);
  else if (strcasestr(str.c_str(), "uniform")) return new Uniform(a1);

  DIE("Unable to create Generator '%s'", str.c_str());

  return NULL;
}
