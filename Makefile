## Copyright (C) 2017 Sahid Orentino Ferdjaoui
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU Lesser General Public
## License as published by the Free Software Foundation; either
## version 2.1 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## Lesser General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public
## License along with this library.  If not, see
## <http://www.gnu.org/licenses/>.

CC=gcc
#CFLAGS=-O2 -std=c99 -D_POSIX_C_SOURCE -lm -lpthread -I.
CFLAGS=-std=c99 -D_POSIX_C_SOURCE -lm -lpthread -I.
DEBUG=-Wall -g -DDEBUG

%.o: src/%.c
	$(CC) -c -o $@ $< $(CFLAGS)

mapred: mr.o mapred.o
	$(CC) -o mapred $^ $(CFLAGS)

debug: mr.o mapred.o
	$(CC) -o mapred $^ $(DEBUG) $(CFLAGS)

valgrind: clean debug
	valgrind --leak-check=full --show-leak-kinds=all ./mapred $(file) $(threads)

trace: clean debug
	strace ./mapred $(file) $(threads)

tests: mr.o tests/distribute.c
	$(CC) -o test $^ $(DEBUG) $(CFLAGS)
	./test

clean:
	rm -f *.o
	rm -f mapred
	rm -f test

.PHONY: clean

