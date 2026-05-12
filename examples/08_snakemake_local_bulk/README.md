# Snakemake Local Bulk Example

This example exists mainly to test and debug Galerna bulk execution before using it on SLURM.

For normal local runs, prefer:

```yaml
run:
  backend: snakemake
  mode: cases
  executor: local
```

This example instead uses:

```yaml
run:
  backend: snakemake
  mode: bulk
  executor: local
  tasks_per_job: 2
  cpus_per_task: 2
  cores: 2
```

Run it from this folder:

```bash
galerna build
galerna run
galerna status
```

Galerna groups cases by `tasks_per_job`. With four cases and `tasks_per_job: 2`, the generated Snakefile creates:

```text
bulk_0000 -> case_0000, case_0001
bulk_0001 -> case_0002, case_0003
```

The concurrency settings have different roles:

- `tasks_per_job: 2` puts two Galerna cases in each bulk group.
- `cpus_per_task: 2` makes each bulk group ask Snakemake for two cores and lets Galerna run up to two case commands inside that group.
- `cores: 2` gives Snakemake a total local budget of two cores.

Because `cores` equals `cpus_per_task`, Snakemake runs one bulk group at a time. If `cores` were `4`, Snakemake could run two bulk groups at once.

Technical done markers are per bulk group:

```text
runs/.galerna/done/bulk_0000.done
runs/.galerna/done/bulk_0001.done
```

Run a complete group:

```bash
galerna run --cases 0-1
```

Partial groups are rejected:

```bash
galerna run --cases 1
```
