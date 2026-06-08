# Snakemake SLURM MPI Cases

This example runs one Galerna case per SLURM job. Each job reserves four MPI
tasks and launches a small MPI executable with `srun`.

Build the executable first:

```bash
make
```

Then submit the Galerna workflow:

```bash
galerna build
galerna run
galerna status
```

The generated Snakemake rules request:

```yaml
threads: 4
resources:
  tasks: 4
  cpus_per_task: 1
```

The `--overlap` flag allows the MPI job step to start inside the SLURM
allocation that Snakemake is already using for its job wrapper.
`PMIX_MCA_psec=native` avoids non-fatal PMIx `psec/munge` warnings on this
cluster.

The case command launches the executable with:

```bash
PMIX_MCA_psec=native srun --overlap --mpi=pmix -n 4 ../../hello_mpi ...
```
