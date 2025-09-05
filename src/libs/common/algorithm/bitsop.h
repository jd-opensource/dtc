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

#ifndef __BITSOP_H__
#define __BITSOP_H__
#include <linux/types.h>
#include <sys/cdefs.h>
__BEGIN_DECLS
/*
   bits operation functions
   */

/* 
 *
 *   FD_* macros provided in select.h set bits by byte on 32-bit machines (assembly implementation), but on 64-bit
 *   machines they set bits by 8 bytes, so when encountering the end of mmap files, segment fault may occur.
 */

#define CHAR_BITS 8

extern const unsigned char __bitcount[];
extern int CountBits(const char *buf, int sz);

//bits op interface
#define SET_B(bit, addr) __set_b(bit, addr)
#define CLR_B(bit, addr) __clr_b(bit, addr)
#define ISSET_B(bit, addr) __isset_b(bit, addr)
#define COPY_B(dest_bit, dest_addr, src_bit, src_addr, count)                  \
	__bit_copy(dest_bit, dest_addr, src_bit, src_addr, count)
#define COUNT_B(buf, size) CountBits(buf, size)

static inline void __set_b(__u32 bit, const volatile void *addr)
{
	volatile __u8 *begin = (volatile __u8 *)addr + (bit / CHAR_BITS);
	__u8 shift = bit % CHAR_BITS;

	*begin |= ((__u8)0x1 << shift);

	return;
}

static inline int __isset_b(__u32 bit, const volatile void *addr)
{
	volatile __u8 *begin = (volatile __u8 *)addr + (bit / CHAR_BITS);
	__u8 shift = bit % CHAR_BITS;

	return (*begin & ((__u8)0x1 << shift)) > 0 ? 1 : 0;
}

static inline void __clr_b(__u32 bit, const volatile void *addr)
{
	volatile __u8 *begin = (volatile __u8 *)addr + (bit / CHAR_BITS);
	__u8 shift = bit % CHAR_BITS;

	*begin &= ~((__u8)0x1 << shift);
}

static inline __u8 __readbyte(const volatile void *addr)
{
	return *(volatile __u8 *)addr;
}
static inline void __writebyte(__u8 val, volatile void *addr)
{
	*(volatile __u8 *)addr = val;
}

static inline void __bit_copy(__u32 dest_bit, volatile void *dest,
			      __u32 src_bit, volatile void *src, __u32 count)
{
	__u32 i;
	for (i = 0; i < count; i++) {
		if (__isset_b(src_bit, src))
			__set_b(dest_bit, dest);
		else
			__clr_b(dest_bit, dest);

		dest_bit++;
		src_bit++;
	}

	return;
}

__END_DECLS
#endif
