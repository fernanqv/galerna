#include <mpi.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int size = 0;
    char hostname[256];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    gethostname(hostname, sizeof(hostname));
    hostname[sizeof(hostname) - 1] = '\0';

    const char *case_id = argc > 1 ? argv[1] : "unknown";
    const char *station = argc > 2 ? argv[2] : "unknown";
    int sleep_seconds = argc > 3 ? atoi(argv[3]) : 0;

    if (sleep_seconds > 0) {
        sleep((unsigned int)sleep_seconds);
    }

    printf(
        "case=%s station=%s rank=%d size=%d host=%s\n",
        case_id,
        station,
        rank,
        size,
        hostname
    );

    MPI_Finalize();
    return 0;
}
