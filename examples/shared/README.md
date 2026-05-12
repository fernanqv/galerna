# Shared Layout Examples

Use `cases.layout: shared` when you want all cases to run in the same output directory. This reduces the number of directories and is useful on filesystems with strict inode limits.

These examples do not use templates. Templates are not required in Galerna; use them only when each case needs generated input files. In shared layout, commands should write outputs with `case_id` or another unique value to avoid overwriting files.

## Path

1. `01_local_no_templates`: sequential local run in one shared directory.
2. `02_snakemake_local_cases`: several case tasks at once on the local machine.
3. `03_snakemake_slurm_cases`: one SLURM job per case.
4. `04_snakemake_slurm_bulk`: grouped cases, with several case commands inside each SLURM job.

Run from any example folder:

```bash
galerna build
galerna run
galerna status
```

Shared layout stores per-case stdout and stderr under `runs/.galerna/logs/`, status under `runs/.galerna/status/`, and technical done markers under `runs/.galerna/done/`.
