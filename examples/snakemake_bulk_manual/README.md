# Manual Snakemake Bulk Example

This example has 64 cases grouped into 2 Snakemake bulk jobs.

- `bulk_0000`: cases `0000` to `0031`
- `bulk_0001`: cases `0032` to `0063`

Each bulk job runs 32 cases and requests 16 cores. Inside the job, the
Snakefile executes up to 16 case commands at the same time.

## Local Dry Run

```bash
cd examples/snakemake_bulk_manual/runs/.galerna
snakemake --dry-run --cores 32
```

## Local Execution

```bash
cd examples/snakemake_bulk_manual/runs/.galerna
snakemake --cores 32
```

## SLURM Execution

From a login node with access to `sbatch`:

```bash
cd examples/snakemake_bulk_manual/runs/.galerna
snakemake --profile profiles/slurm
```

The SLURM profile uses partition `meteo_long`.

## Outputs

Per-case logs:

```text
runs/case_0000/galerna.out
runs/case_0000/galerna.err
runs/case_0000/.galerna.done
```

Bulk logs:

```text
runs/bulk/bulk_0000.log
runs/bulk/bulk_0000.done
```
