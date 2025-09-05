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
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif
#endif

#endif // __HIDDEN
