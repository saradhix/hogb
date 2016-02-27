nvcc -c thrust_multi_lib.cu
g++ -c pgdb.c -I/usr/include/postgresql
nvcc thrust_multi_lib.o pgdb.o -L /usr/lib -lpq
