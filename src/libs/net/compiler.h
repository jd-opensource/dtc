
#ifndef __HIDDEN

#if __GNUC__ >= 4

#define __HIDDEN __attribute__((__visibility__("hidden")))
#define __EXPORT __attribute__((__visibility__("default")))
#define __INLINE inline __attribute__((__always_inline__))
#define HAS_TLS 1

#if __PIC__ || __pic__
#define __TLS_MODEL __attribute__((tls_model("initial-exec")))
#else
#define __TLS_MODEL __attribute__((tls_model("local-exec")))
#endif

#else // __GNUC__
#define __HIDDEN /* */
#define __EXPORT /* */
#define __INLINE inline
#endif // __GNUC__

#ifndef likely
#if __GCC_MAJOR >= 3
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif
#endif

#endif // __HIDDEN
