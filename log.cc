#include <stdio.h>
#include <stdarg.h>

#include "log.h"

log_level_t log_level = INFO;

void log_file_line(log_level_t level, const char *file, int line,
                   const char *format, ...) {
  va_list args;
  char new_format[512];

  snprintf(new_format, sizeof(new_format), "%s(%d): %s\n", file, line, format);

  va_start(args, format);
  if (level >= log_level)
    vfprintf(stderr, new_format, args);
  va_end(args);
}
