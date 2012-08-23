#ifndef LOG_H
#define LOG_H

enum log_level_t { DEBUG, VERBOSE, INFO, WARN, QUIET };
extern log_level_t log_level;

void log_file_line(log_level_t level, const char *file, int line,
                   const char* format, ...);
#define L(level, args...) log_file_line(level, __FILE__, __LINE__, args)

#define D(args...) L(DEBUG, args)
#define V(args...) L(VERBOSE, args)
#define I(args...) L(INFO, args)
#define W(args...) L(WARN, args)

#define DIE(args...) do { W(args); exit(-1); } while (0)

#define NOLOG(x)                                \
  do { log_level_t old = log_level;             \
  log_level = QUIET;                            \
  (x);                                          \
  log_level = old;                              \
  } while (0)

#endif // LOG_H
