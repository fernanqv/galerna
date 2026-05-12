# Galerna Examples

These examples are organized as two learning paths:

- `directories`: one working directory per case.
- `shared`: all cases run from the same output directory.

Templates are optional. They are useful when each case needs generated input files, but Galerna can also run directly from a rendered `command`.

## Three Independent Choices

A Galerna YAML combines three mostly independent choices:

1. Case layout:
   - `directories`: one directory per case.
   - `shared`: all cases run in the same output directory.
2. Input generation:
   - with `templates`: Galerna renders files before running.
   - without `templates`: the command is enough, or inputs already exist.
3. Execution backend:
   - `local`: simple sequential execution.
   - `snakemake`: local parallel execution or SLURM submission.

## Recommended Progression

Start with a local run, then scale the same idea through Snakemake:

```text
local
  -> snakemake local cases
  -> snakemake slurm cases
  -> snakemake slurm bulk
```

In direct local mode, Galerna runs one case at a time:

```yaml
run:
  backend: local
```

In Snakemake local cases mode, several cases can run at once on the local machine:

```yaml
run:
  backend: snakemake
  mode: cases
  executor: local
  cores: 2
```

In Snakemake SLURM cases mode, Snakemake submits one job per case:

```yaml
run:
  backend: snakemake
  mode: cases
  executor: slurm
  max_jobs: 16
  partition: "meteo_long"
```

In Snakemake SLURM bulk mode, Snakemake submits one job per group of cases, and Galerna runs several case commands inside each job:

```yaml
run:
  backend: snakemake
  mode: bulk
  executor: slurm
  tasks_per_job: 16
  cpus_per_task: 16
  max_jobs: 100
  partition: "meteo_long"
```

## Directory Layout Path

Use this path when each simulation should have its own working directory.

| Example | Purpose |
| --- | --- |
| `directories/01_local_with_templates` | Local sequential run with rendered input files |
| `directories/02_local_no_templates` | Local sequential run without templates |
| `directories/03_snakemake_local_cases` | Snakemake local, one task per case |
| `directories/04_snakemake_slurm_cases` | Snakemake SLURM, one job per case |
| `directories/05_snakemake_slurm_bulk` | Snakemake SLURM, grouped cases per job |

Run one:

```bash
cd examples/directories/01_local_with_templates
galerna build
galerna run
galerna status
```

Useful files after a directory-layout run:

```text
runs/.galerna/cases.tsv
runs/<case_id>/galerna.out
runs/<case_id>/galerna.err
runs/<case_id>/galerna.status
runs/<case_id>/.galerna.done
```

For Snakemake bulk mode, the technical done marker is grouped under `runs/.galerna/done/`.

## Shared Layout Path

Use this path when you want to avoid one directory per case.

| Example | Purpose |
| --- | --- |
| `shared/01_local_no_templates` | Local sequential run in one shared directory |
| `shared/02_snakemake_local_cases` | Snakemake local, one task per case |
| `shared/03_snakemake_slurm_cases` | Snakemake SLURM, one job per case |
| `shared/04_snakemake_slurm_bulk` | Snakemake SLURM, grouped cases per job |

Run one:

```bash
cd examples/shared/01_local_no_templates
galerna build
galerna run
galerna status
```

Useful files after a shared-layout run:

```text
runs/.galerna/cases.tsv
runs/.galerna/logs/<case_id>.out
runs/.galerna/logs/<case_id>.err
runs/.galerna/status/status_<group_id>.tsv
runs/.galerna/done/<group_id>.done
```

Commands in shared layout should write unique output names, usually using `{{case_id}}`.

## Advanced

`advanced/custom_build_hook` shows how to subclass `Galerna` and add custom logic in `build_case`.
