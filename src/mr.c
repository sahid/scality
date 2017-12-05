/*
 * Copyright (C) 2017 Sahid Orentino Ferdjaoui
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <search.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

#include "include/mr.h"

// in-memory storage
static struct hentry *storage = NULL;
static size_t storage_index = 0;
static pthread_mutex_t storage_lock;

// cmp function used to find key in storage, O(n).
static int cmp(const void *key, const void *obj)
{
	struct entry *e = (struct entry *)obj;
	return strcmp(key, e->key);
}

// Releases allocated memory for the input_split LL
void input_split_deallocate(struct input_split *input)
{
	struct input_split *next = NULL;
	while (input) {
		next = input->next;
		if (input->value)
			free(input->value);
		free(input);

		input = next;
	}
}

// Releases allocated memory for the in-memory storage
static void storage_deallocate()
{
	for (int i = 0; i < storage_index; i++) {
		if (storage[i].root) {
			struct hentry_value *node = storage[i].root;
			// For each value entry we need to release
			// memory for the value and the node itself.
			while (node) {
				struct hentry_value *tmp = node;
				free(node->value);
				node = node->next;
				free(tmp);
			}
		}
	}
	free(storage);

	storage = NULL;
	storage_index = 0;
}

static int storage_init(int size)
{
	storage = malloc(sizeof(struct hentry) * STORAGE_INITIAL_SIZE);
	if (storage == NULL) {
		fprintf(stderr,
			"Unable to allocate storage memory, %s\n",
			strerror(errno));
		return -1;
	}
	storage_index = 0;
	return 0;
}

static int storage_realloc(int newsize)
{
	storage = realloc(storage, newsize);
	if (storage == NULL) {
		fprintf(stderr,
			"Unable to re-allocate storage memory, %s\n",
			strerror(errno));
		return -1;
	}
	return 0;
}

// TODO(sahid): refactor needed, some part are redondantes and we
// could probably have specific functions to allocate memory of hentry...
int emit(char *key, void *value, unsigned int vsize)
{
	static unsigned int space = 0;
	struct hentry *e = NULL;
	int ret = 0;

	DEBUG_MSG("Emit %s=%p\n", key, value);

	// Global lock... probably not the best way
	pthread_mutex_lock(&storage_lock);

	// Init the storage whether is not already done.  The algo is
	// using a simple array so the performance can not be optimal
	// and a simple search is O(n). We could probably do better by
	// using a htable (hsearch).
	if (storage == NULL) {
		// TODO(sahid): We could use dynamic allocation to decrease memory
		// consumption.
		DEBUG_MSG
		    ("Initialize in-memory storage, default size=%d entries\n",
		     STORAGE_INITIAL_SIZE);
		if (storage_init(STORAGE_INITIAL_SIZE) == -1) {
			ret = -1;
			goto unlock;
		}
		space = STORAGE_INITIAL_SIZE;
	}
	// Search whether the entry already exists. If that the case
	// so we happen the value to the refered key, if not so we
	// have to add new entry in the storage, also cheking whether
	// there is enough space in it or realloc.
	e = (struct hentry *)lfind(key, storage, &storage_index,
				   sizeof(struct hentry), cmp);
	if (e) {
		DEBUG_MSG("Key '%s' found in storage, appening value=%p\n", key,
			  value);
		// At this point we want to append that new value.
		struct hentry_value *curr = e->root;
		while (curr->next) {
			curr = curr->next;
		}
		curr->next = malloc(sizeof(struct hentry_value));
		if (curr->next == NULL) {
			fprintf(stderr, "Unable to allocate value memory, %s\n",
				strerror(errno));
			ret = -1;
			goto unlock;
		}
		curr->next->value = malloc(vsize);
		memcpy(curr->next->value, value, vsize);
		curr->next->next = NULL;
	} else {
		DEBUG_MSG("Key '%s' not found in storage, appening value=%p\n",
			  key, value);
		// First we want to ensure they is enough space is the storage
		if (storage_index >= space) {
			DEBUG_MSG
			    ("Extra space needed in storage idx=%ld, space=%d\n",
			     storage_index, space);
			// We need to increase the size of the storage.
			space = ceil(space * STORAGE_INCR_RATIO);
			if (storage_realloc(sizeof(struct hentry) * space) ==
			    -1) {
				ret = -1;
				goto unlock;
			}
		}
		// Here, we want to add a new hentry in the storage and store the
		// value.
		storage[storage_index].key = key;
		storage[storage_index].root =
		    malloc(sizeof(struct hentry_value));
		if (storage[storage_index].root == NULL) {
			fprintf(stderr, "Unable to allocate value memory, %s\n",
				strerror(errno));
			ret = -1;
			goto unlock;
		}
		storage[storage_index].root->value = malloc(vsize);
		memcpy(storage[storage_index].root->value, value, vsize);
		storage[storage_index].root->next = NULL;

		// Increment the storage index position
		storage_index++;
	}

 unlock:
	pthread_mutex_unlock(&storage_lock);

	return ret;
}

// TODO(sahid): refactor needed some part are redondantes and we could
// have speific function to allocate memory of nodes.
struct input_split *file_input_format_split(void *p)
{
	struct file_input_format_params *params = p;
	struct input_split *root = NULL;
	struct input_split *curr = NULL;
	char *line = NULL;
	unsigned int key = 0;

	// Open the input file in read-only
	FILE *input = fopen(params->filename, "r");
	if (input == NULL) {
		fprintf(stderr, "Can't open input file '%s', %s\n",
			params->filename, strerror(errno));
		goto err;
	}
	// Parse the stream based on the pattern defined by user
	for (;;) {
		fscanf(input, params->pattern, &line);
		if (line == NULL) {
			break;
		}
		DEBUG_MSG("Reading line of %ld characters for key '%d'\n",
			  strlen(line), key);

		// first-loop, we initialize the linked-list and set
		// the value
		if (!root) {
			DEBUG_MSG("Creating root for split, key '%d'\n", key);
			// TODO(sahid): The algo is redondant I should
			// create a function and factorize this a bit.
			root = malloc(sizeof(struct input_split));
			if (root == NULL) {
				fprintf(stderr,
					"Unable to allocate input_split, %s\n",
					strerror(errno));
				goto err;
			}
			root->next = NULL;
			root->key = key;
			root->value = malloc(strlen(line) + 1);
			if (root->value == NULL) {
				fprintf(stderr,
					"Unable to allocate input_split->value, "
					"%s\n", strerror(errno));
				goto err;
			}
			strcpy(root->value, line);
			free(line);

			curr = root;
		} else {
			DEBUG_MSG("Appening new value for split, key '%d'\n",
				  key);
			// Here we want to add new node to the current
			// node
			curr->next = malloc(sizeof(struct input_split));
			if (curr->next == NULL) {
				fprintf(stderr,
					"Unable to allocate input_split, %s\n",
					strerror(errno));
				goto err;
			}
			curr->next->next = NULL;
			curr->next->key = key;
			curr->next->value = malloc(strlen(line) + 1);
			if (curr->next->value == NULL) {
				fprintf(stderr,
					"Unable to allocate input_split->value, "
					"%s\n", strerror(errno));
				goto err;
			}
			strcpy(curr->next->value, line);
			free(line);

			curr = curr->next;
		}
		// Let's incr the node key
		key++;
	}

 err:
	// In all cases if something is wrong before to quit we want
	// to clean the resources allocated. We expect that the caller
	// to free the nodes of the linked-list (struct input_split *)
	// if needed.
	if (input)
		fclose(input);
	if (line)
		free(line);

	return root;
}

void distribute(struct input_split *input, struct input_split *buckets[],
		unsigned int numthreads)
{
	// The 'buckets' is storing the a ref of the first element of
	// the input nodes passed to each map process
	for (int y = 0; y < numthreads; y++) {
		buckets[y] = NULL;
	}

	while (input) {
		// TODO(sahid): needs to check the behavior with
		// non-even list of nodes.
		int key = input->key % numthreads;
		if (!buckets[key]) {
			DEBUG_MSG("Adding root thread process, thread=%d\n",
				  key);
			buckets[key] = input;
			input = input->next;

			buckets[key]->next = NULL;
		} else {
			struct input_split *append = buckets[key];
			while (append->next) {
				append = append->next;
			}
			DEBUG_MSG
			    ("Appending split to thread process, thread=%d\n",
			     key);
			append->next = input;
			input = input->next;

			append->next->next = NULL;
		}
	}
	assert(input == NULL);
}

// Is where everything start
int operate(struct operations *op, void *params, unsigned int numthreads)
{

	if (numthreads < MIN_THREADS || numthreads > MAX_THREADS) {
		fprintf(stderr, "Consider to use a range %d..%d for threads\n",
			MIN_THREADS, MAX_THREADS);
		return -1;
	}

	struct input_split *buckets[numthreads];
	struct input_split *input = NULL;
	pthread_t mthreads[numthreads];
	void *output = NULL;
	unsigned int rsize = 0;
	int ret = 0;

	// Generate inputs wich will be passed to the map function
	input = op->inputify(params);

	distribute(input, buckets, numthreads);

#ifdef DEBUG
	for (int i = 0; i < numthreads; i++) {
		DEBUG_MSG("BUCKET %d\n", i);
		struct input_split *n = buckets[i];
		while (n) {
			DEBUG_MSG("> %p\n", n->value);
			n = n->next;
		}
	}
#endif

	// Init the lock
	if (pthread_mutex_init(&storage_lock, NULL) != 0) {
		fprintf(stderr, "Unable to init lock, %s\n", strerror(errno));
		ret = -1;
		goto free;
	}
	// Start the threads of map functions
	for (int i = 0; i < numthreads; i++) {
		if (pthread_create(&mthreads[i], NULL, op->map, buckets[i])) {
			fprintf(stderr, "Unable to create thread %d, %s\n", i,
				strerror(errno));
			ret = -1;
			goto free;

		}
		DEBUG_MSG("Started thread: %d\n", i);
	}

	// Wait for all threads before to start reducing phase
	for (int i = 0; i < numthreads; i++) {
		if (pthread_join(mthreads[i], NULL)) {
			fprintf(stderr, "Unable to join thread %i\n", i);
			ret = -1;
			goto free;
		}
		DEBUG_MSG("Finished thread: %d\n", i);
	}

	// The map process have finished their job with the documents
	// split to we can freing the resources allocated.
	rsize = op->reduce(storage, storage_index, &output);
	DEBUG_MSG("Reducing phase produced %d elements\n", rsize);

	op->outputify(output, rsize);

 free:
	// Release lock
	pthread_mutex_destroy(&storage_lock);

	// Release inputs
	for (int i = 0; i < numthreads; i++) {
		input_split_deallocate(buckets[i]);
	}

	// Release in-memory storage.
	storage_deallocate();

	return ret;
}
