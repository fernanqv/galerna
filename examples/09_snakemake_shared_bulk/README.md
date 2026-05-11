# Snakemake Shared Bulk Example

This example combines shared layout with Snakemake bulk execution.

```yaml
cases:
  layout: shared

run:
  backend: snakemake
  mode: bulk
  executor: local
  tasks_per_job: 2
  cpus_per_task: 2
  cores: 2
```

Use this to test the low-inode bulk workflow before moving the same idea to SLURM.

Run it from this folder:

```bash
galerna build
galerna run
galerna status
```

All case commands run from the same `runs/` directory, so outputs must include `{{ case_id }}`:

```text
runs/result_case_0000.txt
runs/result_case_0001.txt
```

Galerna groups cases by `tasks_per_job`:

```text
bulk_0000 -> case_0000, case_0001
bulk_0001 -> case_0002, case_0003
```

Status files are grouped by bulk group:

```text
runs/.galerna/status/status_bulk_0000.tsv
runs/.galerna/status/status_bulk_0001.tsv
```

Technical done markers are also grouped:

```text
runs/.galerna/done/bulk_0000.done
runs/.galerna/done/bulk_0001.done
```

Run one complete bulk group:

```bash
galerna run --cases 0-1
galerna status
```

Partial groups are rejected:

```bash
galerna run --cases 1
```
