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
 * 模拟异步文件
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
		 * 向前移动v bytes
		 */
		inline void Front(int v) {
			MEMORY_BARRIER();
			serial = serial;
			offset += v;
			MEMORY_BARRIER();
		}
		/*
		 *  递增一个文件编号
		 */
		inline void Shift(void) {
			MEMORY_BARRIER();
			offset = 0;
			/*
			 * 有可能在这个点出现暂态，读者会认为自己GT写者，从而出错
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
		 *  切换文件时，有可能出现暂态出错，见Shift中的解释
		 *
		 *  因为是无锁判定，所以在读者判定时，我们先调用IsTransient来检测是否暂态，如果是则sespend读者
		 */
		inline int IsTransient(const CAsyncFilePos &v) {
			return (serial == v.serial) && (offset > 0) && (v.offset == 0);
		}

}__attribute__ ((packed));

typedef CAsyncFilePos CReaderPos;
typedef CAsyncFilePos CWriterPos;

#endif
