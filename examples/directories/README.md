# Directory Layout Examples

Use `cases.layout: directories` when each case should have its own working directory. This is the most comfortable layout when a model expects local input files and writes outputs in the current directory.

Templates are optional. The first example renders files into each case directory; the second example shows the same layout without templates.

## Path

1. `01_local_with_templates`: sequential local run with rendered input files.
2. `02_local_no_templates`: sequential local run without templates.
3. `03_snakemake_local_cases`: several cases at once on the local machine.
4. `04_snakemake_slurm_cases`: one SLURM job per case.
5. `05_snakemake_slurm_bulk`: grouped cases, with several case commands inside each SLURM job.
6. `06_snakemake_slurm_mpi_cases`: one SLURM job per case running a 4-rank MPI executable.

Run from any example folder:

```bash
galerna build
galerna run
galerna status
```

For local debugging, start with `01_local_with_templates` or `02_local_no_templates`. To scale up, move to Snakemake local, then SLURM cases, then SLURM bulk.
