#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#include <inttypes.h>
#include <limits.h>
#include <math.h>

#include "Generator.h"
#include "util.h"

int main(int argc, char **argv) {
  //  double now = get_time();
  //  uint64_t x = fnv_64_buf(&now, sizeof(now));

  srand48(0xdeadbeef);

  /*
  Generator *n = createGenerator("n:1,1"); // new Normal(1, 1);
  Generator *e = createGenerator("e:1"); // new Exponential(1);
  Generator *p = createGenerator("p:214.476,0.348238"); // new GPareto(214.476, 0.348238);
  Generator *g = createGenerator("g:30.7984,8.20449,0.078688"); // new GEV(30.7984, 8.20449, 0.078688);

  printf("%f\n", n->generate());
  printf("%f\n", e->generate());
  printf("%f\n", p->generate());
  printf("%f\n", g->generate());

  srand48(0);

  printf("\n\n");

  Discrete *d = new Discrete(createGenerator("p:214.476,0.348238"));
  //  d->add(.5, -1.0);
  //Generator *d = createGenerator("p:214.476,0.348238");

  for (int i = 0; i < 20; i++) {
        printf("d %d\n", (int) d->generate());
  }

  printf("\n\n");
  srand48(0);

  //Discrete *d2 = new Discrete(createGenerator("p:214.476,0.348238"));
  //  d->add(.5, -1.0);
  Generator *d2 = createGenerator("p:214.476,0.348238");

  for (int i = 0; i < 20; i++) {
    printf("d %d\n", (int) d2->generate());
  }

  KeyGenerator kg(g);
  */

  Generator *g = createFacebookValue();
  //  Generator *g = createGenerator("pareto:15,214.476,0.348238");
  for (int i = 0; i < 1000000; i++)
    printf("%f\n", g->generate());

  /*
  Generator *p2 = createGenerator("p:214.476,0.348238");
  //  for (int i = 0; i < 1000; i++)
  //    printf("%f\n", p2->generate());

  p2->set_lambda(1000);
  for (int i = 0; i < 1000; i++)
    printf("%f\n", p2->generate());
  */

  //  for (int i = 0; i < 10000; i++)
  //    printf("%s\n", kg.generate(i).c_str());

  /*
  for (uint64_t ind = 0; ind < 10000; ind++) {
    //  uint64_t ind = 0;
    uint64_t h = fnv_64(ind);
    double U = (double) h / ULLONG_MAX;
  //  double E = e->generate(U); // -log(U);
    double G = g->generate(U);
    int keylen = MAX(round(G), floor(log10(10000)) + 1);

    //    printf("ind=%" PRIu64 "\n", ind);
    //    printf("h=%" PRIu64 "\n", h);
    //    printf("U=%f\n", U);
    //    printf("G=%f\n", G);
    //    printf("keylen=%d\n", keylen);
    printf("%7" PRIu64 " %7d key=%0*" PRIu64 "\n", ind, keylen, keylen, ind);
  }
  */
}
