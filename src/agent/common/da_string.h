/*
 * Copyright JD.com, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DA_STRING_H_
#define DA_STRING_H_

#include "../da_util.h"
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

/*
 * warning:you must manage the memory yourself
 */
struct string {
  uint32_t len;  /* string length */
  uint8_t *data; /* string data */
};

#define string(_str)                                                           \
  { sizeof(_str) - 1, (uint8_t *)(_str) }
#define null_string                                                            \
  { 0, NULL }

#define string_set_text(_str, _text)                                           \
  do {                                                                         \
    (_str)->len = (uint32_t)(sizeof(_text) - 1);                               \
    (_str)->data = (uint8_t *)(_text);                                         \
  } while (0);

#define string_set_raw(_str, _raw)                                             \
  do {                                                                         \
    (_str)->len = (uint32_t)(da_strlen(_raw));                                 \
    (_str)->data = (uint8_t *)(_raw);                                          \
  } while (0);

void string_init(struct string *str);
void string_deinit(struct string *str);
bool string_empty(const struct string *str);
int string_duplicate(struct string *dst, const struct string *src);
int string_copy(struct string *dst, const uint8_t *src, uint32_t srclen);
int string_compare(const struct string *s1, const struct string *s2);

bool string_upper(struct string* str);
bool string_lower(struct string* str);

static inline uint8_t *_da_strchr(uint8_t *p, uint8_t *last, uint8_t c) {
  while (p < last) {
    if (*p == c) {
      return p;
    }
    p++;
  }
  return NULL;
}

static inline uint8_t *_da_strrchr(uint8_t *p, uint8_t *start, uint8_t c) {
  while (p >= start) {
    if (*p == c) {
      return p;
    }
    p--;
  }

  return NULL;
}

#define da_strcpy(_d, _s) strcpy((char *)_d, (char *)_s)
#define da_strncpy(_d, _s, _n) strncpy((char *)_d, (char *)_s, (size_t)(_n))
#define da_strcat(_d, _s) strcat((char *)_d, (char *)_s)
#define da_memcpy(_d, _c, _n) memcpy(_d, _c, (size_t)(_n))
#define da_memmove(_d, _c, _n) memmove(_d, _c, (size_t)(_n))
#define da_memchr(_d, _c, _n) memchr(_d, _c, (size_t)(_n))
#define da_strlen(_s) strlen((char *)(_s))
#define da_strncmp(_s1, _s2, _n)                                               \
  strncmp((char *)(_s1), (char *)(_s2), (size_t)(_n))
#define da_strchr(_p, _l, _c)                                                  \
  _da_strchr((uint8_t *)(_p), (uint8_t *)(_l), (uint8_t)(_c))
#define da_strrchr(_p, _s, _c)                                                 \
  _da_strrchr((uint8_t *)(_p), (uint8_t *)(_s), (uint8_t)(_c))
#define da_strndup(_s, _n) (uint8_t *)strndup((char *)(_s), (size_t)(_n));
#define da_snprintf(_s, _n, ...)                                               \
  snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)
#define da_scnprintf(_s, _n, ...)                                              \
  _scnprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)
#define da_vscnprintf(_s, _n, _f, _a)                                          \
  _vscnprintf((char *)(_s), (size_t)(_n), _f, _a)
#define da_strftime(_s, _n, fmt, tm)                                           \
  (int)strftime((char *)(_s), (size_t)(_n), fmt, tm)
#define da_safe_snprintf(_s, _n, ...)                                          \
  _safe_snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)
#define da_safe_vsnprintf(_s, _n, _f, _a)                                      \
  _safe_vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

/*
 * A (very) limited version of snprintf
 * @param   to   Destination buffer
 * @param   n    Size of destination buffer
 * @param   fmt  printf() style format string
 * @returns Number of bytes written, including terminating '\0'
 * Supports 'd' 'i' 'u' 'x' 'p' 's' conversion
 * Supports 'l' and 'll' modifiers for integral types
 * Does not support any width/precision
 * Implemented with simplicity, and async-signal-safety in mind
 */
int _safe_vsnprintf(char *to, size_t size, const char *format, va_list ap);
int _safe_snprintf(char *to, size_t n, const char *fmt, ...);

#endif /* DA_STRING_H_ */
