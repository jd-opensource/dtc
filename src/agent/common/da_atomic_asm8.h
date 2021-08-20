#ifndef __ARCH_I386_ATOMIC8__
#define __ARCH_I386_ATOMIC8__

#include <sys/cdefs.h>
__BEGIN_DECLS

/*
 * cmpxchg8b: require Pentium or above
 *    compare edx:eax with target memory,
 *    if equal, write ecx:ebx to memory
 *    if non-equal, keep old value in edx:eax and write back
 */

/*
 * Make sure gcc doesn't try to be clever and move things around
 * on us. We need to use _exactly_ the address the user gave us,
 * not some alias that contains the same information.
 */
typedef struct { volatile long long counter; } atomic8_t;

#define ATOMIC8_INIT(i)	{ (i) }

/**
 * atomic8_read - read atomic variable
 * @v: pointer of type atomic8_t
 * 
 * Atomically reads the value of @v.
 */ 
static __inline__ long long atomic8_read(atomic8_t *v)
{
	register long long r;
	__asm__ __volatile__(
	/* original value for ebx, ecx doesn't matter */
		"       movl            %%ecx, %%edx\n"
		"       movl            %%ebx, %%eax\n"
		"lock;  cmpxchg8b       %1\n"
		: "=&A" (r), "+o" (v->counter)
		:
		: "memory", "cc");
	return r;
}

/**
 * atomic8_set - set atomic variable
 * @v: pointer of type atomic8_t
 * @i: required value
 * 
 * Atomically sets the value of @v to @i.
 */ 
static __inline__ long long atomic8_return_set(atomic8_t *v, long long i)
{
	register long long r = v->counter;
#if __pic__ || __PIC__
	__asm__ __volatile__(
		"       pushl           %%ebx\n"
		"       movl            %2, %%ebx\n"
		"1:     repz; nop\n" // aka pause
		"lock;  cmpxchg8b       (%1)\n"
		"       jnz             1b\n"
		"       popl            %%ebx\n"
			: "+A"(r)
			: "ir"(v), "ir" ((int)i), "c"((int)((unsigned long long)i>>32))
			: "memory", "cc");
#else
	__asm__ __volatile__(
		"1:     repz; nop\n" // aka pause
		"lock;  cmpxchg8b       %1\n"
		"       jnz             1b\n"
			: "+A"(r), "+o" (v->counter)
			: "b" ((int)i), "c"((int)((unsigned long long)i>>32))
			: "memory", "cc");
#endif
	return r;
}

static __inline__ void atomic8_set(atomic8_t *v, long long i)
{
	atomic8_return_set(v, i);
}

/**
 * atomic8_set - set atomic variable
 * @v: pointer of type atomic8_t
 * @i: required value
 * 
 * Atomically sets the value of @v to @i.
 */ 
static __inline__ long long atomic8_clear(atomic8_t *v)
{
	return atomic8_return_set(v, 0);
}

/**
 * atomic8_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type atomic8_t
 * 
 * Atomically adds @i to @v.
 */
static __inline__ long long atomic8_add(long long i, atomic8_t *v)
{
	register long long r = v->counter;
#if __pic__ || __PIC__
	__asm__ __volatile__(
		"       pushl           %%ebx\n"
		"1:     movl            %2, %%ebx\n"
		"       movl            %3, %%ecx\n"
		"       addl            %%eax, %%ebx\n"
		"       adcl            %%edx, %%ecx\n"
		"repz;  nop\n"
		"lock;  cmpxchg8b       (%1)\n"
		"       jnz             1b\n"
		"       popl            %%ebx\n"
			: "+A" (r)
			: "ir"(v), "ir" ((int)i), "ir"((int)((unsigned long long)i>>32))
			: "memory", "ecx", "cc");
#else
	__asm__ __volatile__(
		"1:     movl            %2, %%ebx\n"
		"       movl            %3, %%ecx\n"
		"       addl            %%eax, %%ebx\n"
		"       adcl            %%edx, %%ecx\n"
		"repz;  nop\n"
		"lock;  cmpxchg8b       %1\n"
		"       jnz             1b\n"
			: "+A"(r), "+o" (v->counter)
			: "g" ((int)i), "g"((int)((unsigned long long)i>>32))
			: "memory", "ebx", "ecx", "cc");
#endif
	return r;
}

/**
 * atomic8_add_return - add and return
 * @v: pointer of type atomic8_t
 * @i: integer value to add
 *
 * Atomically adds @i to @v and returns @i + @v
 */
static __inline__ long long atomic8_add_return(long long i, atomic8_t *v)
{
	register long long r = v->counter;
#if __pic__ || __PIC__
	__asm__ __volatile__(
		"       pushl           %%ebx\n"
		"1:     movl            %2, %%ebx\n"
		"       movl            %3, %%ecx\n"
		"       addl            %%eax, %%ebx\n"
		"       adcl            %%edx, %%ecx\n"
		"repz;  nop\n"
		"lock;  cmpxchg8b       (%1)\n"
		"       jnz             1b\n"
		"       movl            %%ebx, %%eax\n"
		"       movl            %%ecx, %%edx\n"
		"       popl            %%ebx\n"
			: "+A" (r)
			: "ir"(v), "ir" ((int)i), "ir"((int)((unsigned long long)i>>32))
			: "memory", "ecx", "cc");
#else
	__asm__ __volatile__(
		"1:     movl            %2, %%ebx\n"
		"       movl            %3, %%ecx\n"
		"       addl            %%eax, %%ebx\n"
		"       adcl            %%edx, %%ecx\n"
		"repz;  nop\n"
		"lock;  cmpxchg8b       %1\n"
		"       jnz             1b\n"
		"       movl            %%ebx, %%eax\n"
		"       movl            %%ecx, %%edx\n"
			: "+A" (r), "+o" (v->counter)
			: "g" ((int)i), "g"((int)((unsigned long long)i>>32))
			: "memory", "ebx", "ecx", "cc");
#endif
	return r;
}

/**
 * atomic8_sub - subtract the atomic variable
 * @i: integer value to subtract
 * @v: pointer of type atomic8_t
 * 
 * Atomically subtracts @i from @v.
 */
static __inline__ long long atomic8_sub(long long i, atomic8_t *v)
{
	return atomic8_add(-i, v);
}

/**
 * atomic8_sub_and_test - subtract value from variable and test result
 * @i: integer value to subtract
 * @v: pointer of type atomic8_t
 * 
 * Atomically subtracts @i from @v and returns
 * true if the result is zero, or false for all
 * other cases.
 */
static __inline__ int atomic8_sub_and_test(long long i, atomic8_t *v)
{
	return atomic8_add_return(-i, v)==0;
}

/**
 * atomic8_inc - increment atomic variable
 * @v: pointer of type atomic8_t
 * 
 * Atomically increments @v by 1.
 */ 
static __inline__ long long atomic8_inc(atomic8_t *v)
{
	return atomic8_add(1, v);
}

/**
 * atomic8_dec - decrement atomic variable
 * @v: pointer of type atomic8_t
 * 
 * Atomically decrements @v by 1.
 */ 
static __inline__ long long atomic8_dec(atomic8_t *v)
{
	return atomic8_add(-1, v);
}

/**
 * atomic8_dec_and_test - decrement and test
 * @v: pointer of type atomic8_t
 * 
 * Atomically decrements @v by 1 and
 * returns true if the result is 0, or false for all other
 * cases.
 */ 
static __inline__ int atomic8_dec_and_test(atomic8_t *v)
{
	return atomic8_add_return(-1, v)==0;
}

/**
 * atomic8_inc_and_test - increment and test 
 * @v: pointer of type atomic8_t
 * 
 * Atomically increments @v by 1
 * and returns true if the result is zero, or false for all
 * other cases.
 */ 
static __inline__ int atomic8_inc_and_test(atomic8_t *v)
{
	return atomic8_add_return(1, v)==0;
}

/**
 * atomic8_add_negative - add and test if negative
 * @v: pointer of type atomic8_t
 * @i: integer value to add
 * 
 * Atomically adds @i to @v and returns true
 * if the result is negative, or false when
 * result is greater than or equal to zero.
 */ 
static __inline__ int atomic8_add_negative(long long i, atomic8_t *v)
{
	return atomic8_add_return(i, v) < 0;
}

static __inline__ long long atomic8_sub_return(long long i, atomic8_t *v)
{
	return atomic8_add_return(-i,v);
}

#define atomic8_inc_return(v)  (atomic8_add_return(1,v))
#define atomic8_dec_return(v)  (atomic8_sub_return(1,v))

__END_DECLS
#endif
