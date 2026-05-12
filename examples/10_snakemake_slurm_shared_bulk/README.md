# Snakemake SLURM Shared Bulk Example

This example is a small cluster test for Galerna's Snakemake SLURM executor path.

```yaml
cases:
  layout: shared

run:
  backend: snakemake
  mode: bulk
  executor: slurm
  tasks_per_job: 4
  cpus_per_task: 16
  max_jobs: 1
  partition: "meteo_long"
```

It creates one bulk group:

```text
bulk_0000 -> case_0000, case_0001, case_0002, case_0003
```

Snakemake submits that group as one SLURM job to `meteo_long`. Inside the job, Galerna can run up to `cpus_per_task` case commands concurrently. This example has only four cases, so it runs four case commands inside the one SLURM job.

Run it on the cluster from this folder:

```bash
galerna build
galerna run
galerna status
```

Expected outputs:

```text
runs/result_case_0000.txt
runs/result_case_0001.txt
runs/result_case_0002.txt
runs/result_case_0003.txt
runs/.galerna/status/status_bulk_0000.tsv
runs/.galerna/done/bulk_0000.done
```

The result files include the hostname where the case command ran.
