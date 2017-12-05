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

#include "include/mr.h"

// This is a simple example of using libmr to compute words of input
// document.

void usage(char *prgm, int status)
{
	if (status != EXIT_SUCCESS) {
		fprintf(stdout, "Usage: %s <FILE> <THREADS>\n", prgm);
	}
	exit(status);
}

// Receives linked-list of lines from the text documents. This
// functions will split the lines by words and emit the result. We
// consider the word as the key.
void *scality_map(void *in)
{
	struct input_split *input_split = in;
	static short value = 1;

	while (input_split) {
		DEBUG_MSG("Map executed for: '%p'\n", input_split->value);

		char *context;
		char *word = strtok_r(input_split->value, " ,.", &context);
		while (word != NULL) {
			emit(word, &value, sizeof(char) * strlen(word) + 1);
			word = strtok_r(NULL, " ,.", &context);
		}
		input_split = input_split->next;
	}
	return NULL;
}

struct scality_output {
	char *word;
	short count;
};

// Receives ref of in-memory storage, will parse each key and sum the
// values. The result will be stored in a array of 'scality_output'
// which will be then passed to the 'outputify' operation.
unsigned int scality_reduce(struct hentry *storage, unsigned int size,
			    void **output)
{
	struct scality_output *o = NULL;

	o = malloc(sizeof(struct scality_output) * size);
	if (o == NULL) {
		fprintf(stderr, "Unable to allocate memory for reducer o, %s\n",
			strerror(errno));
		return 0;
	}

	for (int i = 0; i < size; i++) {
		unsigned int sum = 0;
		struct hentry_value *curr = storage[i].root;
		while (curr) {
			sum += *((short *)curr->value);
			curr = curr->next;
		}
		o[i].word = storage[i].key;
		o[i].count = sum;
	}

	*output = o;

	return size;
}

int cmp(const void *o1, const void *o2)
{
	const struct scality_output *entry1 = o1;
	const struct scality_output *entry2 = o2;
	return strcmp(entry1->word, entry2->word);
}

// Receives array of struct scality_output, this function is only
// responsible of sorting it and printing the result.
int scality_output(void *reduced, unsigned int size)
{
	struct scality_output *data = reduced;

	// A requirement was to sort the result
	qsort(data, size, sizeof(struct scality_output), cmp);
	for (int i = 0; i < size; i++) {
		fprintf(stdout, "%s=%d\n", data[i].word, data[i].count);
	}
	free(data);
	return 0;
}

int main(int argc, char **argv)
{
	char *prgmname = argv[0];

	if (argc != 3) {
		usage(prgmname, EXIT_FAILURE);
	}

	int numthreads = strtol(argv[2], NULL, 10);

	struct file_input_format_params input_params = {
		argv[1],
		"%m[^\n]\n",	// Possible overflow if the line is too big
	};

	struct operations scality_op = {
		file_input_format_split,	// Defined for free by libmr

		scality_map,
		scality_reduce,
		scality_output
	};

	// Start the job
	if (operate(&scality_op, &input_params, numthreads) == -1) {
		usage(argv[0], EXIT_FAILURE);
	}

	return 0;
}
