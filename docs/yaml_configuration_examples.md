# Galerna YAML Configuration Examples

This document sketches the intended YAML interface for Galerna execution. It is a design guide for the first clean execution architecture, not a guarantee that every field is already implemented.

## Core Ideas

Galerna separates case generation from execution:

- `command`: command for one case, rendered with the case context.
- `cases.layout`: where case commands run.
- `run.backend`: execution backend used by Galerna.
- `run.executor`: execution system used by the backend, when relevant.
- `run.workflow.source`: whether Galerna generates the workflow or the user provides one.

SLURM is not a Galerna backend. Use:

```yaml
run:
  backend: snakemake
  executor: slurm
```

not:

```yaml
run:
  backend: slurm
```

## Defaults And Minimal YAML

The simplest valid generated-workflow YAML is:

```yaml
variable_parameters:
  station: [1, 2, 3, 4]

command: "python run_model.py {{station}}"
```

It is equivalent to:

```yaml
output_dir: "output"
mode: "one_by_one"

cases:
  layout: directories
  id_format: '{{ "%04d" | format(case_num) }}'

variable_parameters:
  station: [1, 2, 3, 4]

command: "python run_model.py {{station}}"

run:
  backend: local
  mode: cases
  executor: local
  cores: 1
  cpus_per_task: 1
  tasks_per_job: 1
  workflow:
    source: generated

status:
  mode: auto
```

## 1. Direct Local Debug Run

Use this for debugging and quick checks. Galerna runs cases sequentially, with no local parallelism.

```yaml
templates_dir: "templates"
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: directories

variable_parameters:
  station: [1, 2, 3, 4]

command: "python ../../../dummy_script.py {{station}}"

run:
  backend: local
```

Expected behavior:

```text
case 0000 finishes
case 0001 finishes
case 0002 finishes
case 0003 finishes
```

## 2. Snakemake Local, One Task Per Case

Use this when you want local parallel execution managed by Snakemake.

```yaml
templates_dir: "templates"
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: directories

variable_parameters:
  station: [1, 2, 3, 4, 5, 6, 7, 8]

command: "python ../../../dummy_script.py {{station}}"

run:
  backend: snakemake
  mode: cases
  executor: local
  cores: 4
  workflow:
    source: generated
```

Galerna generates:

```text
runs/.galerna/cases.tsv
runs/.galerna/Snakefile
```

Each Snakemake task runs one case from its case directory.

## 3. Snakemake SLURM, One Job Per Case

Use this when each case should become a separate SLURM job.

```yaml
templates_dir: "templates"
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: directories

variable_parameters:
  station: [1, 2, 3, 4, 5, 6, 7, 8]

command: "python ../../../dummy_script.py {{station}}"

run:
  backend: snakemake
  mode: cases
  executor: slurm
  max_jobs: 20
  cpus_per_task: 1
  partition: "meteo_long"
  runtime: 30
  mem_mb: 1000
  workflow:
    source: generated
```

Conceptually:

```text
case 0000 -> SLURM job
case 0001 -> SLURM job
case 0002 -> SLURM job
```

## 4. Snakemake SLURM Bulk, 32 Cases Per Job With 16 Cores

Use this when a cluster policy requires large jobs, or when submitting one job per case is inefficient.

```yaml
templates_dir: "templates"
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: directories

variable_parameters:
  station: "range(1, 129)"

command: "python ../../../dummy_script.py {{station}}"

run:
  backend: snakemake
  mode: bulk
  executor: slurm
  tasks_per_job: 32
  max_jobs: 10
  cpus_per_task: 16
  partition: "meteo_long"
  runtime: 120
  mem_mb: 4000
  workflow:
    source: generated
```

Conceptually:

```text
bulk 0000 -> cases 0000-0031 -> 16 cores
bulk 0001 -> cases 0032-0063 -> 16 cores
bulk 0002 -> cases 0064-0095 -> 16 cores
bulk 0003 -> cases 0096-0127 -> 16 cores
```

Inside each bulk job, Galerna's generated Snakemake rule can execute up to `cpus_per_task` case commands concurrently.

## 5. Shared Layout To Reduce Inode Use

Use this when the filesystem has strict inode limits and creating one folder per case is too expensive.

In shared layout:

- all commands run from `output_dir`;
- Galerna does not render per-case templates;
- the user command must create unique output names using `case_id`, `case_num`, or other parameters;
- logs and status files are stored under `output_dir/.galerna/`.

```yaml
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: shared
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: [1, 2, 3, 4]

command: "python run_model.py --station {{station}} --output result_{{case_id}}.txt"

run:
  backend: snakemake
  mode: cases
  executor: local
  cores: 4
  workflow:
    source: generated
```

Expected output layout:

```text
runs/
  result_case_0000.txt
  result_case_0001.txt
  .galerna/
    cases.tsv
    Snakefile
    done/
      cases.done
    logs/
      case_0000.out
      case_0000.err
    status/
      status_cases.tsv
```

Avoid commands like this in shared layout:

```yaml
command: "python run_model.py --output result.txt"
```

because cases would overwrite each other.

## 6. Shared Layout With SLURM Bulk

Shared layout can also be combined with bulk execution.

```yaml
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: shared
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: "range(1, 129)"

command: "python run_model.py --station {{station}} --output result_{{case_id}}.txt"

run:
  backend: snakemake
  mode: bulk
  executor: slurm
  tasks_per_job: 32
  max_jobs: 10
  cpus_per_task: 16
  partition: "meteo_long"
  runtime: 120
  mem_mb: 4000
  workflow:
    source: generated
```

This avoids per-case directories while still letting Snakemake submit grouped SLURM jobs.

## 7. User-Provided Snakefile

Advanced users can provide their own Snakefile. In this mode, `command` is optional because the user workflow defines the execution logic.

Galerna still builds cases and exports a manifest.

```yaml
templates_dir: "templates"
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: directories

variable_parameters:
  station: [1, 2, 3, 4]

run:
  backend: snakemake
  executor: slurm
  workflow:
    source: user
    file: "workflow/Snakefile"
    profile: "workflow/profiles/slurm"
  manifest: "runs/.galerna/cases.tsv"
```

The user Snakefile is responsible for reading `cases.tsv`.

## 8. Custom Build Logic With Inheritance

For model-specific build logic, use a Python class that inherits from `Galerna`. This is the right extension point when templates are not enough, for example to compute derived parameters, generate auxiliary files, or prepare case-specific inputs.

YAML:

```yaml
wrapper:
  code: "aux/xbeach_wrapper.py"
  class: "XbeachWrapper"

templates_dir: "holland_template"
output_dir: "holland_output"
mode: "all_combinations"

cases:
  layout: directories
  id_format: "holland_{{ var1 }}_{{ var2 }}_{{ np }}_{{ compiler }}"

variable_parameters:
  var1: [225, 226]
  var2: [514, 315]
  np: [1, 2]
  compiler: ["gfortran", "ifort"]

command: "bash run_xbeach.sh {{compiler}} {{np}}"

run:
  backend: local
```

Python wrapper:

```python
from pathlib import Path

from galerna import Galerna


class XbeachWrapper(Galerna):
    def build_case(self, case_context: dict) -> None:
        case_dir = Path(case_context["case_dir"])
        derived_value = case_context["var1"] + case_context["var2"]
        (case_dir / "derived.txt").write_text(f"{derived_value}\n")
```

`build_case(case_context)` is called after the case directory has been created and before templates are rendered.

Keep this mechanism for real Python logic. For ordinary text inputs, prefer `templates_dir` and Jinja templates. Symlinks inside `templates_dir` remain supported for `cases.layout: directories`.

For `cases.layout: shared`, Galerna does not render per-case templates, so custom build logic should avoid creating one file per case unless that is intentional.

## 9. Future Nextflow Backend

The same structure leaves room for a future Nextflow backend.

```yaml
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: shared
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: [1, 2, 3, 4]

run:
  backend: nextflow
  executor: slurm
  workflow:
    source: user
    file: "workflow/main.nf"
    config: "workflow/nextflow.config"
  manifest: "runs/.galerna/cases.csv"
```

For a first Galerna version, this can remain a reserved interface rather than an implemented backend.

## Status Files

Generated workflows should write status lines so `galerna status` can work without depending on SLURM commands such as `squeue` or `sacct`.

For directory layout:

```text
runs/0000/galerna.status
runs/0000/.galerna.done
```

Suggested content:

```tsv
timestamp	status	message
2026-05-10T11:55:00Z	BUILT	case built
2026-05-10T12:00:00Z	STARTED	
2026-05-10T12:03:10Z	DONE	exit_code=0
```

For shared layout:

```text
runs/.galerna/status/status_cases.tsv
runs/.galerna/status/status_bulk_0000.tsv
runs/.galerna/done/cases.done
runs/.galerna/done/bulk_0000.done
```

Suggested content:

```tsv
timestamp	case_id	status	message
2026-05-10T11:55:00Z	case_0000	BUILT	case built
2026-05-10T12:00:00Z	case_0000	STARTED	
2026-05-10T12:03:10Z	case_0000	DONE	exit_code=0
```

For shared bulk execution, prefer one status file per bulk group instead of one global file. This keeps inode use low while avoiding many jobs appending concurrently to the same file.

`galerna.status` and `status_<group_id>.tsv` are human/historical logs. `.galerna.done` and `.galerna/done/<group_id>.done` are technical success markers for Snakemake and should only be created after the corresponding case or group succeeds.

Users may append their own status lines after Galerna has run. Galerna should reserve execution statuses such as `BUILT`, `STARTED`, `DONE`, `FAILED`, and future technical statuses such as `SKIPPED`, but it should not reject custom statuses.

For example, a user or post-run script could append:

```tsv
timestamp	status	message
2026-05-10T12:10:00Z	QC_OK	output checked manually
2026-05-10T12:15:00Z	TRANSFERRED	copied to archive
```

For shared layout, include `case_id`:

```tsv
timestamp	case_id	status	message
2026-05-10T12:10:00Z	case_0000	QC_OK	output checked manually
```

The rule for users should be: append new lines, do not edit or delete existing status history. Custom statuses are useful for manual QA, transfers, archiving, or project-specific review stages. They do not replace `.galerna.done`, which remains the workflow-engine marker for technical completion.

Suggested status logic for `galerna status`:

- `NOT_BUILT`: the case is in the manifest, but no status line exists for it.
- `BUILT`: latest status line is `BUILT`; the case has been generated but not started.
- `DONE`: latest status line is `DONE`.
- `FAILED`: latest status line is `FAILED`.
- `RUNNING`: latest status line is `STARTED` or another active custom status.

`galerna status` exposes both views:

- default: latest human status, including custom user statuses;
- `galerna status --execution`: latest Galerna-reserved execution status, ignoring later custom statuses.
