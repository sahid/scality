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

#ifndef _MR_H_
#define _MR_H_

#define MIN_THREADS 1
#define MAX_THREADS 100

// Considering update these values to increase perofrmance
#define STORAGE_INITIAL_SIZE 64
#define STORAGE_INCR_RATIO 1.25

#ifdef DEBUG
#define PRINT_DEBUG 1
#else
#define PRINT_DEBUG 0
#endif

#define DEBUG_MSG(fmt, ...)\
	if (PRINT_DEBUG)\
		fprintf(stdout, "%s:%d:%s(): " fmt, __FILE__, __LINE__, __func__, __VA_ARGS__)

// The input documents are splited, that operation is done by the
// 'operations->inputify' function. The result is a linked-list of
// struct input_split *' elements which will be then distributed to
// the map function according the number of threads.
struct input_split {
	unsigned int key;
	void *value;

	struct input_split *next;
};

// The hentry* are parts of the in-memory storage used to record the
// key/value data emitted by the map processes. The storage is a
// simple array where each value contain a 'struct hentry' (basically
// the key) then each key is linked to a linked-list of values
// (hentry_value).
//
//  0 [key1]->A->B->C->NULL
//  1 [key2]->A->NULL
//  . [ ...]
//  N [keyN]->A->B->NULL
//
struct hentry {
	char *key;

	struct hentry_value *root;
};

struct hentry_value {
	void *value;

	struct hentry_value *next;
};

// Definining users operations.
struct operations {
	// Split input documents
	struct input_split *(*inputify) (void *);

	// Takes a document input_splits which are going to be emit by
	// key/value. A struct input_split * is passed to it but this
	// function is directly used by pthread_create.
	void *(*map) (void *);

	// Returns pointer of input data which will be passed to the
	// output operation. The 'storage' as to be seen like a
	// key/value datastore.
	unsigned int (*reduce) (struct hentry *, unsigned int, void **);

	// Takes result from the 'reduce' operation to output it
	// within different format.
	int (*outputify) (void *, unsigned int);
};

// Distributing the linked-list of inputs to the threads. The design
// is perhaps a bit weird, using a linked-list was perhaps not the
// optimal solution but i don't have the time to rewrite it :/ The
// algo is about to split the LL accros a bucket of threads which are
// going to execute the map function.
void distribute(struct input_split *, struct input_split *[], unsigned int);

// The emit is storing key/value in the in-memory storage, if the key
// already exists so the value is appenned. The storage uses a default
// size STORAGE_INITIAL_SIZE, and increases its size if necessary by
// STORAGE_INCR_RATIO. Considering to adjust this for performance.
// The emit operation is thread-safe.
int emit(char *, void *, unsigned int);

// The function is sheduling the operations:
//
// 1. Split the document
// 2. Distribute the chunks of documents accros the map functions
// 3. Call the reducer
// 4. Execute the output job iwth result of the reducer
// 5. Release resources
int operate(struct operations *op, void *input, unsigned int numthreads);

// We provide for free function to parse text based documents
struct file_input_format_params {
	char *filename;
	char *pattern;
};

// Based on the params `struct file_input_format_params *` this
// function split a document and returns a linked-list of `struct
// input_split *`. Be aware of that the caller is reponsible to free
// the allocated resources. Is taking ref of file_input_format_params
struct input_split *file_input_format_split(void *);

#endif
