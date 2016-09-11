CC=gcc
MCC=mpicc
CFLAGS=-c -Wall
CFL=-Wall
CFF=-fopenmp

all: generate examine

generate: generator.o
	$(CC) generator.o -o generator.out	
	./generator.out data.txt 15000000
	
generator.o: generator.c
	$(CC) $(CFLAGS) generator.c

examine: 
	$(MCC) $(CFL) $(CFF) -o examinempi.out examinempi.c 
	mpirun examinempi.out -1 -1 data.txt -1 -1 	

clean: 
	rm -rf *.o
	rm -rf *.out




