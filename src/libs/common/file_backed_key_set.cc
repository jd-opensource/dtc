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
#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif

#ifndef __USE_LARGEFILE64
#define __USE_LARGEFILE64
#endif

#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/fcntl.h>

#include <cstring>
#include <cassert>
#include <cstdlib>

#include "algorithm/new_hash.h"
#include "log/log.h"
#include "file_backed_key_set.h"

FileBackedKeySet::hash_node_allocator::~hash_node_allocator()
{
	reset();
}

void FileBackedKeySet::hash_node_allocator::reset()
{
	for (std::vector<char *>::iterator iter = m_buffs.begin();
	     iter != m_buffs.end(); ++iter) {
		::free(*iter);
	}
	m_buffs.clear();
	m_freeNode = 0;
}

FileBackedKeySet::hash_node *FileBackedKeySet::hash_node_allocator::alloc()
{
	if (!m_freeNode) {
		FileBackedKeySet::hash_node *nodes =
			(hash_node *)calloc(GROW_COUNT, sizeof(hash_node));

		FileBackedKeySet::hash_node *n = nodes;
		for (int i = 0; i < GROW_COUNT - 1; ++i) {
			n->next = n + 1;
			++n;
		}
		m_freeNode = nodes;
		m_buffs.push_back((char *)nodes);
	}

	hash_node *rtn = m_freeNode;
	m_freeNode = (hash_node *)m_freeNode->next;
	return rtn;
}

void FileBackedKeySet::hash_node_allocator::free(hash_node *n)
{
	n->next = m_freeNode;
	m_freeNode = n;
}

FileBackedKeySet::FileBackedKeySet(const char *file, int keySize)
	: m_filePath(file), m_fd(-1), m_keySize(keySize),
	  m_base((char *)MAP_FAILED), m_buckets(0)
{
}

FileBackedKeySet::~FileBackedKeySet()
{
	if (m_fd >= 0) {
		if (m_base != MAP_FAILED)
			munmap(m_base, get_meta_info()->size);
		close(m_fd);
	}

	if (m_buckets)
		free(m_buckets);
}

int FileBackedKeySet::Open()
{
	assert(m_fd < 0);
	assert(m_base == MAP_FAILED);

	m_fd = open(m_filePath.c_str(), O_RDWR | O_CREAT | O_LARGEFILE, 0644);
	if (m_fd < 0) {
		log4cplus_error("open %s failed, %m", m_filePath.c_str());
		return -1;
	}

	//discard all content if any
	if (ftruncate(m_fd, 0) == -1) {
		// Handle error - could close fd and return
	}
	if (ftruncate(m_fd, INIT_FILE_SIZE) == -1) {
		// Handle error - could close fd and return
	}

	m_base = (char *)mmap(NULL, INIT_FILE_SIZE, PROT_READ | PROT_WRITE,
			      MAP_SHARED, m_fd, 0);
	if (m_base == MAP_FAILED) {
		log4cplus_error("mmap failed, %m");
		close(m_fd);
		m_fd = -1;

		return -1;
	}

	meta_info *m = get_meta_info();
	m->size = INIT_FILE_SIZE;
	m->writePos = sizeof(meta_info);

	m_buckets = (hash_node **)calloc(BUCKET_SIZE, sizeof(hash_node *));

	return 0;
}

int FileBackedKeySet::Load()
{
	assert(m_fd < 0);
	assert(m_base == MAP_FAILED);

	m_fd = open(m_filePath.c_str(), O_RDWR | O_LARGEFILE);
	if (m_fd < 0) {
		log4cplus_error("open %s failed, %m", m_filePath.c_str());
		return -1;
	}

	struct stat64 st;
	int rtn = fstat64(m_fd, &st);
	if (rtn < 0) {
		log4cplus_error("fstat64 failed, %m");
		close(m_fd);
		m_fd = -1;
		return -1;
	}

	m_base = (char *)mmap(NULL, st.st_size, PROT_READ | PROT_WRITE,
			      MAP_SHARED, m_fd, 0);
	if (m_base == (char *)MAP_FAILED) {
		log4cplus_error("mmap failed, %m");
		close(m_fd);
		m_fd = -1;
		return -1;
	}

	m_buckets = (hash_node **)calloc(BUCKET_SIZE, sizeof(hash_node *));

	char *start = m_base + sizeof(meta_info);
	char *end = m_base + get_meta_info()->writePos;

	//variable key size
	if (m_keySize == 0) {
		while (start < end) {
			int keyLen = *(unsigned char *)start + 1;
			insert_to_set(start + 1, keyLen + 1);
			start += keyLen + 1 + 1;
		}
	} else {
		while (start < end) {
			insert_to_set(start + 1, m_keySize);
			start += m_keySize + 1;
		}
	}

	return 0;
}

int FileBackedKeySet::do_insert(const char *key)
{
	if (Contains(key, false))
		return 0;

	int keyLen = m_keySize == 0 ? (*(unsigned char *)key) + 1 : m_keySize;
	char *k = insert_to_file(key, keyLen);
	if (!k) //remap failed?
		return -1;
	insert_to_set(k, keyLen);
	return 0;
}

bool FileBackedKeySet::Contains(const char *key, bool checkStatus)
{
	int keyLen = m_keySize == 0 ? (*(unsigned char *)key) + 1 : m_keySize;
	uint32_t hash = new_hash(key, keyLen) % BUCKET_SIZE;
	hash_node *n = m_buckets[hash];
	while (n) {
		char *k = m_base + n->offset;
		if (memcmp(key, k, keyLen) == 0) {
			if ((checkStatus == true &&
			     *(k - 1) == MIGRATE_SUCCESS) ||
			    checkStatus == false)
				return true;
			break;
		}
		n = n->next;
	}
	return false;
}

bool FileBackedKeySet::is_migrating(const char *key)
{
	int keyLen = m_keySize == 0 ? (*(unsigned char *)key) + 1 : m_keySize;
	uint32_t hash = new_hash(key, keyLen) % BUCKET_SIZE;
	hash_node *n = m_buckets[hash];
	while (n) {
		char *k = m_base + n->offset;
		if (memcmp(key, k, keyLen) == 0) {
			if (*(k - 1) == MIGRATE_START)
				return true;
			break;
		}
		n = n->next;
	}
	// key not found
	return false;
}

int FileBackedKeySet::Migrated(const char *key)
{
	int keyLen = m_keySize == 0 ? (*(unsigned char *)key) + 1 : m_keySize;
	uint32_t hash = new_hash(key, keyLen) % BUCKET_SIZE;
	hash_node *n = m_buckets[hash];
	while (n) {
		char *k = m_base + n->offset;
		if (memcmp(key, k, keyLen) == 0) {
			*(k - 1) = MIGRATE_SUCCESS;
			return 0;
		}
		n = n->next;
	}
	// key not found
	return -1;
}

void FileBackedKeySet::insert_to_set(const char *key, int len)
{
	uint32_t hash = new_hash(key, len) % BUCKET_SIZE;
	hash_node *n = m_allocator.alloc();
	n->next = m_buckets[hash];
	n->offset = key - m_base;
	m_buckets[hash] = n;
}

char *FileBackedKeySet::insert_to_file(const char *key, int len)
{
	meta_info *m = get_meta_info();
	if (m->writePos + len + 1 > m->size) {
		uintptr_t new_size = m->size + GROW_FILE_SIZE;
		if (ftruncate(m_fd, new_size) < 0) {
			log4cplus_error("grow file to %p failed, %m",
					(void *)new_size);
			return NULL;
		}

		char *base = (char *)mremap(m_base, m->size, new_size,
					    MREMAP_MAYMOVE);
		if (base == MAP_FAILED) {
			log4cplus_error("mremap failed, %m");
			return NULL;
		}

		m_base = base;
		m = get_meta_info();
		m->size = new_size;
	}

	char *writePos = m_base + m->writePos;
	*writePos = MIGRATE_START;
	++writePos;
	memcpy(writePos, key, len);
	m->writePos += len + 1;

	return writePos;
}

void FileBackedKeySet::Clear()
{
	if (m_fd >= 0) {
		free(m_buckets);
		m_allocator.reset();
		munmap(m_base, get_meta_info()->size);
		close(m_fd);
		m_buckets = 0;
		m_base = (char *)MAP_FAILED;
		m_fd = -1;

		unlink(m_filePath.c_str());
	}
}
