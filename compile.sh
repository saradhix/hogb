nvcc -c thrust_sort.cu
g++ -c pgdb.c -I/usr/include/postgresql $1
nvcc thrust_sort.o pgdb.o -L /usr/lib -lpq
