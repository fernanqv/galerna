MPI_MODULE ?= openmpi-5.0.5/gcc-8.5.0-slurm-ogwvq
MPICC ?= mpicc
CFLAGS ?= -O2 -Wall -Wextra

.PHONY: all clean

all: hello_mpi

hello_mpi: hello_mpi.c
	module load $(MPI_MODULE) && $(MPICC) $(CFLAGS) -o $@ $<

clean:
	rm -f hello_mpi
