# Galerna YAML Configuration Examples

This document describes the Galerna v1 YAML interface. It focuses on the implemented execution model:

- `run.backend: local`
- `run.backend: snakemake`
- `run.executor: local`
- `run.executor: slurm`
- `run.mode: cases`
- `run.mode: bulk`

SLURM is not a Galerna backend. Use Snakemake as the backend and SLURM as the executor:

```yaml
run:
  backend: snakemake
  executor: slurm
```

## Three Independent Choices

A Galerna configuration combines three mostly independent choices.

Case layout:

```yaml
cases:
  layout: directories
```

or:

```yaml
cases:
  layout: shared
```

Input generation:

```yaml
templates_dir: "templates"
```

or no templates at all.

Execution:

```yaml
run:
  backend: local
```

or:

```yaml
run:
  backend: snakemake
  mode: cases
  executor: local
  snakemake:
    cli:
      cores: 4
```

Templates are optional. Use them when Galerna should render input files before running each case. If the command can use rendered variables directly, or if inputs already exist, omit `templates_dir`.

## Defaults And Minimal YAML

The simplest useful YAML is:

```yaml
variable_parameters:
  station: [1, 2, 3, 4]

command: "python run_model.py {{station}}"
```

It uses these defaults:

```yaml
output_dir: "output"
mode: "one_by_one"

cases:
  layout: directories
  id_format: "{{ '%04d' | format(case_num) }}"

run:
  backend: local
```

Direct local execution is sequential. It runs one case at a time.

Galerna looks for `galerna.yaml` in the current directory when `--config` is not provided.

## Variable Parameters

`variable_parameters` defines the cases.

With `mode: one_by_one`, all parameter lists must have the same length:

```yaml
mode: "one_by_one"

variable_parameters:
  station: [1, 2, 3]
  sleep_seconds: [5, 10, 15]
```

This creates three cases:

```text
case 0 -> station=1, sleep_seconds=5
case 1 -> station=2, sleep_seconds=10
case 2 -> station=3, sleep_seconds=15
```

You can also use string ranges:

```yaml
variable_parameters:
  station: range(1,101)
  sleep_seconds: range(1,101)
```

With `mode: all_combinations`, Galerna creates the Cartesian product:

```yaml
mode: "all_combinations"

variable_parameters:
  station: [1, 2]
  compiler: ["gcc", "intel"]
```

This creates four cases:

```text
station=1, compiler=gcc
station=1, compiler=intel
station=2, compiler=gcc
station=2, compiler=intel
```

Use `fixed_parameters` for values shared by all cases:

```yaml
fixed_parameters:
  sleep_seconds: 1
```

## Directory Layout

`cases.layout: directories` creates one working directory per case. This is the default and the best starting point when a model expects local files in its working directory.

```yaml
output_dir: "runs"

cases:
  layout: directories

variable_parameters:
  station: [10, 20, 30]

command: "python run_model.py --station {{station}}"

run:
  backend: local
```

Expected layout after running:

```text
runs/
  0000/
    galerna.out
    galerna.err
    .galerna.done
  0001/
    galerna.out
    galerna.err
    .galerna.done
  .galerna/
    cases.tsv
    status/
      status_0000.tsv
      status_0001.tsv
```

## Directory Layout With Templates

Templates are useful when each case needs generated input files.

```yaml
templates_dir: "templates"
output_dir: "runs"
mode: "all_combinations"

cases:
  layout: directories
  id_format: "station_{{ station }}_{{ compiler }}"

variable_parameters:
  station: [1, 2]
  compiler: ["gcc", "intel"]

fixed_parameters:
  sleep_seconds: 1

command: "python run_case.py"

run:
  backend: local
```

For a template file `templates/input.txt`:

```text
station={{ station }}
compiler={{ compiler }}
sleep_seconds={{ sleep_seconds }}
case_id={{ case_id }}
```

Galerna renders the template into each case directory before running `command`.

## Shared Layout

`cases.layout: shared` runs all cases from the same `output_dir`. This reduces directory creation and is useful on filesystems with strict inode limits.

```yaml
output_dir: "runs"

cases:
  layout: shared
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: [100, 200, 300]

command: "python run_model.py --station {{station}} --output result_{{case_id}}.txt"

run:
  backend: local
```

Commands in shared layout must write unique output names. Usually that means using `{{case_id}}`.

Avoid:

```yaml
command: "python run_model.py --output result.txt"
```

because cases would overwrite each other.

Expected layout:

```text
runs/
  result_case_0000.txt
  result_case_0001.txt
  .galerna/
    cases.tsv
    logs/
      case_0000.out
      case_0000.err
    status/
      status_cases.tsv
    done/
      cases.done
```

## Snakemake Local Cases

Use Snakemake local execution when you want to run several cases at once on the local machine.

```yaml
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: directories
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: [101, 102, 103, 104]
  sleep_seconds: [1, 1, 1, 1]

command: "python run_model.py --station {{station}}"

run:
  backend: snakemake
  mode: cases
  executor: local
  snakemake:
    cli:
      cores: 2
```

Each Galerna case becomes one Snakemake task. `snakemake.cli.cores` is the total local core budget Snakemake may use.

Galerna generates:

```text
runs/.galerna/cases.tsv
runs/.galerna/Snakefile
```

The same idea works with shared layout:

```yaml
cases:
  layout: shared

run:
  backend: snakemake
  mode: cases
  executor: local
  snakemake:
    cli:
      cores: 2
```

## Snakemake SLURM Cases

Use this when each case should become a separate SLURM job.

```yaml
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: shared
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: range(1,17)
  sleep_seconds: range(1,17)

command: "python run_model.py --station {{station}} --output result_{{case_id}}.txt"

run:
  backend: snakemake
  mode: cases
  executor: slurm
  snakemake:
    rule:
      resources:
        runtime: 10
        mem_mb: 1000
        slurm_partition: "meteo_long"
```

Conceptually:

```text
case_0000 -> SLURM job
case_0001 -> SLURM job
case_0002 -> SLURM job
```

`snakemake.rule.resources` is rendered inside each generated Snakemake rule.
For SLURM, Galerna passes `--jobs unlimited` unless `snakemake.cli.jobs` is
provided.
Use `snakemake.cli.default-resources` only when you want Snakemake-level
fallback resources instead of per-rule resources.

Run this on a system where SLURM and the Snakemake SLURM executor plugin are available.

## Snakemake SLURM Bulk

Use bulk mode when submitting one job per case is inefficient, or when a cluster policy requires jobs with several cores.

```yaml
output_dir: "runs"
mode: "one_by_one"

cases:
  layout: shared
  id_format: "case_{{ '%04d' | format(case_num) }}"

variable_parameters:
  station: range(1,101)
  sleep_seconds: range(1,101)

command: "python run_model.py --station {{station}} --output result_{{case_id}}.txt"

run:
  backend: snakemake
  mode: bulk
  executor: slurm
  cases_per_job: 16
  snakemake:
    rule:
      threads: 16
      resources:
        runtime: 10
        mem_mb: 1000
        slurm_partition: "meteo_long"
    cli:
      jobs: 100
```

Conceptually:

```text
bulk_0000 -> case_0000 ... case_0015
bulk_0001 -> case_0016 ... case_0031
...
bulk_0006 -> case_0096 ... case_0099
```

The parameters mean:

- `cases_per_job`: number of Galerna cases grouped into one Snakemake job.
- `snakemake.rule.threads`: threads requested by each Snakemake job, and maximum number of case commands Galerna may run concurrently inside that job.
- `snakemake.cli.jobs`: maximum number of Snakemake jobs submitted or active at once.

With `cases_per_job: 16` and `threads: 16`, each full bulk job runs 16 case commands at the same time inside one SLURM allocation.

## Snakemake Local Bulk

Bulk mode also works with `executor: local`, mainly to test the bulk workflow before moving to SLURM. For normal local parallel execution, prefer `mode: cases`.

```yaml
run:
  backend: snakemake
  mode: bulk
  executor: local
  cases_per_job: 2
  snakemake:
    rule:
      threads: 2
    cli:
      cores: 2
```

With four cases:

```text
bulk_0000 -> cases 0000, 0001
bulk_0001 -> cases 0002, 0003
```

`snakemake.cli.cores` is the local Snakemake budget. `snakemake.rule.threads` is the size of each bulk job. In local bulk mode:

```text
number of simultaneous bulk jobs ~= floor(cli.cores / rule.threads)
```

## Selecting Cases

Use `--cases` with comma-separated indices or ranges:

```bash
galerna build --cases 0-3
galerna run --cases 1,3
galerna status --cases 0,2-4
```

The manifest always contains all cases. `--cases` selects which cases to build, run, or show.

In Snakemake bulk mode, `--cases` must select complete groups. For example, with `cases_per_job: 2`, `--cases 0-1` is valid but `--cases 1` is rejected because it would select only part of `bulk_0000`.

## Status Files

Galerna writes append-only status logs. The important distinction is:

- `status_<group_id>.tsv`: human/historical logs under `.galerna/status/`.
- `.galerna.done` and `.galerna/done/*.done`: technical success markers for local execution and workflow engines.

`galerna status` reads the manifest and status files. It does not need to query Snakemake or SLURM.

Reserved Galerna states include:

- `NOT_BUILT`: calculated by `galerna status` when a case is in the manifest but has no status line.
- `BUILT`: the case was generated by `build`.
- `STARTED`: execution started.
- `DONE`: execution completed successfully.
- `FAILED`: execution failed.

Users may append custom status lines after Galerna has run. Galerna should accept states such as `QC_OK`, `TRANSFERRED`, or `ARCHIVED`.

Status history can be disabled for large campaigns:

```yaml
status:
  mode: none
```

With `status.mode: none`, Galerna does not write `BUILT`, `STARTED`, `DONE`, or
`FAILED` rows. `galerna status` infers `DONE` from the done marker and otherwise
reports a manifest-backed `BUILT` state.

For both directory and shared layout, status rows include `case_id`:

```tsv
timestamp	case_id	status	message
2026-05-10T11:55:00Z	case_0000	BUILT	case built
2026-05-10T12:00:00Z	case_0000	STARTED	
2026-05-10T12:03:10Z	case_0000	DONE	exit_code=0
2026-05-10T12:10:00Z	case_0000	QC_OK	output checked manually
```

Logs can be discarded independently. The corresponding manifest field is
written as `/dev/null`:

```yaml
logs:
  stdout: discard
  stderr: file
```

The rule for users is: append new lines, do not edit or delete existing status history. Custom statuses do not replace `.galerna.done`, which remains the workflow-engine marker for technical completion.

`galerna status` exposes two views:

```bash
galerna status
galerna status --execution
```

Default `galerna status` reports the latest human status, including custom user statuses. `galerna status --execution` reports the latest Galerna-reserved execution status, ignoring later custom statuses.

## Custom Build Logic With Inheritance

For model-specific build logic, use a Python class that inherits from `Galerna`. This is the right extension point when templates are not enough, for example to compute derived parameters, generate auxiliary files, or prepare case-specific inputs.

YAML:

```yaml
wrapper:
  code: "custom_wrapper.py"
  class: "CustomWrapper"

templates_dir: "templates"
output_dir: "runs"

cases:
  layout: directories

variable_parameters:
  station: [1, 2]

command: "python run_case.py"

run:
  backend: local
```

Python wrapper:

```python
from pathlib import Path

from galerna import Galerna


class CustomWrapper(Galerna):
    def build_case(self, case_context: dict) -> None:
        case_dir = Path(case_context["case_dir"])
        derived = case_context["station"] * 10
        case_context["derived"] = derived
        (case_dir / "derived.txt").write_text(f"{derived}\n")
```

`build_case(case_context)` is called after the case directory has been created and before templates are rendered.

Keep this mechanism for real Python logic. For ordinary text inputs, prefer `templates_dir` and Jinja templates.

## Example Folders

The executable examples are organized as two learning paths:

```text
examples/directories/
examples/shared/
```

Start with local execution, then move to Snakemake local, then SLURM cases, then SLURM bulk:

```text
local
  -> snakemake local cases
  -> snakemake slurm cases
  -> snakemake slurm bulk
```

The advanced wrapper example is in:

```text
examples/advanced/custom_build_hook/
```
