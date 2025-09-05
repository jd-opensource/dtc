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
#ifndef DA_ATOMIC__T
#define DA_ATOMIC__T

#include <stdint.h>

#if __GNUC__ < 4
#include "da_atomic_asm.h"
#else
#include "da_atomic_gcc.h"
#endif

#if __WORDSIZE == 64 || __GNUC__ >= 5 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3)
#define HAS_ATOMIC8 1
#include "da_atomic_gcc8.h"
#else
#define HAS_ATOMIC8 1
#include "da_atomic_asm8.h"
#endif

#endif
