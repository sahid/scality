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
#include <assert.h>

#include "include/mr.h"

// Just about to test the algo to distribute the input linked-list for
// the threads which are going to execute the map function.

int main() {
	char *values[] = {"aa", "bb", "cc", "dd",
			  "ee", "ff", "gg", "hh"};
	unsigned int svalues = sizeof(values) / sizeof(char *);

        // Generate list of input;
	struct input_split *root = NULL;
	struct input_split *curr = NULL;
	for (int i=0; i<svalues; i++) {
		if (!root) {
			root = malloc(sizeof(struct input_split));
			root->value = &values[i];
			root->next = NULL;

			curr = root;
		} else {
			curr->next = malloc(sizeof(struct input_split));
			curr->next->value = &values[i];
			curr->next->next = NULL;

			curr = curr->next;
		}
		curr->key = i;
	}

	for (int threads=1; threads<=2; threads++) {
		struct input_split *buckets[threads];
		distribute(root, buckets, threads);
		for (int y=0; y<threads; y++) {
			int counter = 0;
			struct input_split *n = buckets[y];
			while(n) {
				n = n->next;
				counter ++;
			}
			assert(counter = svalues / threads);
		}
	}

	return 0;
}
