/*
 * =====================================================================================
 *
 *       Filename:  afile_pos.h
 *
 *    Description:  afile_pos class definition.
 *
 *        Version:  1.0
 *        Created:  04/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */


#ifndef __HB_ASYNCFILE_POS_H
#define __HB_ASYNCFILE_POS_H

/* 
 * 
 * to prevent the compiler optimization 
 * 
 */
#if __GNUC__ < 4
# define MEMORY_BARRIER() __asm__ __volatile__("" : : : "memory")
#else
# define MEMORY_BARRIER() __sync_synchronize()
#endif

/*
 * Simulate asynchronous file
 */
struct CAsyncFilePos {
	public:
		uint32_t serial;
		uint32_t offset;

	public:
		CAsyncFilePos() {
			MEMORY_BARRIER();
			serial = 0;
			offset = 0;
			MEMORY_BARRIER();
		}

		CAsyncFilePos(const CAsyncFilePos & v) {
			MEMORY_BARRIER();
			serial = v.serial;
			offset = v.offset;
			MEMORY_BARRIER();
		}

		/*
		 * Move forward v bytes
		 */
		inline void Front(int v) {
			MEMORY_BARRIER();
			serial = serial;
			offset += v;
			MEMORY_BARRIER();
		}
		/*
		 * Increment a file number
		 */
		inline void Shift(void) {
			MEMORY_BARRIER();
			offset = 0;
			/*
			 * Transient state may occur at this point where reader thinks it's GT writer, causing errors
			 */
			serial += 1;
			MEMORY_BARRIER();
		}

		inline int EQ(const CAsyncFilePos & v) {
			return serial == v.serial && offset == v.offset;
		}

		inline int GT(const CAsyncFilePos & v) {
			return (serial > v.serial) || ((serial == v.serial)
					&& (offset > v.offset) && v.offset != 0);
		}

		inline int Zero() {
			return serial == 0 && offset == 0;
		}

		/*
		 * When switching files, transient errors may occur, see explanation in Shift
		 *
		 * Since it's lock-free determination, when reader determines, we first call IsTransient to detect if transient, if so suspend reader
		 */
		inline int IsTransient(const CAsyncFilePos &v) {
			return (serial == v.serial) && (offset > 0) && (v.offset == 0);
		}

}__attribute__ ((packed));

typedef CAsyncFilePos CReaderPos;
typedef CAsyncFilePos CWriterPos;

#endif
