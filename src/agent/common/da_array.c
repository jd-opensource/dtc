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

#include "da_array.h"
#include "../da_log.h"
#include "../da_util.h"

struct array *array_create(uint32_t n, size_t size) {
	struct array *a;

	ASSERT(n != 0 && size != 0);

	a = malloc(sizeof(*a));
	if (a == NULL) {
		return NULL;
	}

	a->elem = malloc(n * size);
	if (a->elem == NULL) {
		free(a);
		return NULL;
	}

	a->nelem = 0;
	a->size = size;
	a->nalloc = n;

	return a;
}

void array_destroy(struct array *a) {
	array_deinit(a);
	free(a);
}

int array_init(struct array *a, uint32_t n, size_t size) {
	ASSERT(n != 0 && size != 0);

	a->elem = malloc(n * size);
	if (a->elem == NULL) {
		log_error("allocate memory error,no enough memory");
		return -1;
	}

	a->nelem = 0;
	a->size = size;
	a->nalloc = n;

	return 0;
}

void array_deinit(struct array *a) {
	ASSERT(a->nelem == 0);

	if (a->elem != NULL) {
		free(a->elem);
	}
}

uint32_t array_idx(struct array *a, void *elem) {
	uint8_t *p, *q;
	uint32_t off, idx;

	ASSERT(elem >= a->elem);

	p = (uint8_t*)a->elem;
	q = (uint8_t*)elem;
	off = (uint32_t) (q - p);

	ASSERT(off % (uint32_t) a->size == 0);

	idx = off / (uint32_t) a->size;

	return idx;
}

void * array_push(struct array *a) {
	void *elem; 
	void *da_new;
	size_t size;

	if (a->nelem == a->nalloc) {

		/* the array is full; allocate new array */
		size = a->size * a->nalloc;
		da_new = realloc(a->elem,2 * size);
		if (da_new == NULL) {
			return NULL;
		}

		a->elem = da_new;
		a->nalloc *= 2;
	}

	elem =a->elem + a->size * a->nelem;
	a->nelem++;

	return elem;
}

void *
array_pop(struct array *a) {
	void *elem;

	ASSERT(a->nelem != 0);

	a->nelem--;
	elem = (uint8_t *) a->elem + a->size * a->nelem;

	return elem;
}

void *
array_get(struct array *a, uint32_t idx) {
	void *elem;

	ASSERT(a->nelem != 0);
	ASSERT(idx < a->nelem);

	elem = (uint8_t *) a->elem + (a->size * idx);

	return elem;
}

void *
array_top(struct array *a) {
	ASSERT(a->nelem != 0);

	return array_get(a, a->nelem - 1);
}

void array_swap(struct array *a, struct array *b) {
	struct array tmp;

	tmp = *a;
	*a = *b;
	*b = tmp;
}

/*
 * Sort nelem elements of the array in ascending order based on the
 * compare comparator.
 */
void array_sort(struct array *a, array_compare_t compare) {
	ASSERT(a->nelem != 0);

	qsort(a->elem, a->nelem, a->size, compare);
}

/*
 * Calls the func once for each element in the array as long as func returns
 * success. On failure short-circuits and returns the error status.
 */
int array_each(struct array *a, array_each_t func, void *data) {
	uint32_t i, nelem;

	ASSERT(array_n(a) != 0);
	ASSERT(func != NULL);

	for (i = 0, nelem = array_n(a); i < nelem; i++) {
		void *elem = array_get(a, i);
		int status;

		status = func(elem, data);
		if (status != 0) {
			return status;
		}
	}
	return 0;
}

