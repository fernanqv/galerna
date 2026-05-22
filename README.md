# Galerna

Galerna builds and runs parametric numerical-model cases from a small YAML file.

The usual workflow is:

```bash
galerna build
galerna run
galerna status
```

`build` creates the case manifest and any case inputs. `run` executes the selected cases. `status` reads Galerna's status files and reports which cases are built, running, done, failed, or in a user-defined state.

## Acknowledgments

Project EasyFlood: Emulating Automatically SYstems of coastal FLOODing project EasyFlood (PID2023-150689OB-I00) financed by MCIN/AEI/10.13039/501100011033/ ERDF, EU.

<img width="30%" alt="image" src="https://github.com/user-attachments/assets/ec34ff25-15b9-4d0d-b061-5f62e4241d8f" />

## Installation

For development with Snakemake support:

```bash
mamba env create -f environment.yml
mamba activate galerna
pip install -e .
```

For a minimal editable install:

```bash
pip install -e .
```

## Minimal YAML

Create a `galerna.yaml` file:

```yaml
variable_parameters:
  station: [1, 2, 3, 4]

command: "python run_model.py {{ station }}"
```

Then run:

```bash
galerna build
galerna run
galerna status
```

When no `--config` is provided, Galerna looks for `galerna.yaml` in the current directory.

## Core Concepts

- `variable_parameters`: values used to create cases.
- `command`: command for one case, rendered with Jinja using the case context.
- `templates_dir`: optional folder of files rendered into each case directory.
- `cases.layout`: case storage layout, either `directories` or `shared`.
- `run.backend`: execution backend, currently `local` or `snakemake`.
- `run.mode`: Snakemake execution shape, `cases` or `bulk`.
- `run.executor`: Snakemake executor, currently `local` and `slurm`.

## Parameters

`variable_parameters` defines the values that change between cases. By default,
Galerna uses `mode: "one_by_one"`, so all parameter lists are zipped together and
must have the same length:

```yaml
mode: "one_by_one"

variable_parameters:
  station: [1, 2, 3]
  sleep_seconds: [5, 10, 15]
```

This creates:

```text
case 0 -> station=1, sleep_seconds=5
case 1 -> station=2, sleep_seconds=10
case 2 -> station=3, sleep_seconds=15
```

Use `mode: "all_combinations"` when you want the Cartesian product of all
values:

```yaml
mode: "all_combinations"

variable_parameters:
  station: [1, 2]
  compiler: ["gcc", "intel"]
```

This creates four cases: all station/compiler combinations.

For long integer sequences, a parameter value can be a `range(start, stop)` or
`range(start, stop, step)` string:

```yaml
variable_parameters:
  station: range(1,101)
  sleep_seconds: range(10,1010,10)
```

`variable_parameters` can also point to a separate YAML file. This is useful
when the case matrix is large:

```yaml
variable_parameters: "parameters.yaml"

command: "python run_model.py {{ station }} {{ scenario }}"
```

where `parameters.yaml` contains the parameter mapping:

```yaml
station: [1, 2, 3]
scenario: ["base", "storm", "surge"]
```

`fixed_parameters` defines values shared by every case. They are added to the
same Jinja context as the variable parameters, so they can be used in
`command`, `cases.id_format`, and templates:

```yaml
variable_parameters:
  station: [1, 2, 3]

fixed_parameters:
  model: "ww3"
  sleep_seconds: 1

command: "python run_model.py --model {{ model }} --station {{ station }}"
```

Avoid reusing the same key in `variable_parameters` and `fixed_parameters`; a
fixed value is global and should not be used for per-case variation.

## Execution Modes

### Direct Local

Use this for debugging and simple sequential runs.

```yaml
run:
  backend: local
```

Direct local execution runs one case at a time.

### Snakemake Local Cases

Use this when you want local parallel execution managed by Snakemake.

```yaml
run:
  backend: snakemake
  mode: cases
  executor: local
  snakemake:
    cli:
      cores: 4
```

Each Galerna case becomes one Snakemake task. `snakemake.cli.cores` is passed
to Snakemake as the local core budget.

### Snakemake Local Bulk

Bulk local execution exists mainly to test the bulk workflow before using it on a cluster.

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

For normal local execution, prefer `mode: cases`. Bulk mode groups cases and creates one technical done marker per group.

The three bulk parameters control different levels of concurrency:

- `cases_per_job`: group size. It says how many Galerna cases belong to one bulk Snakemake job.
- `snakemake.rule.threads`: job size. It says how many threads each bulk job asks Snakemake for, and how many case commands Galerna may run concurrently inside that bulk job.
- `snakemake.cli.cores`: local Snakemake budget. It says how many cores Snakemake may use in total on the current machine.

With the example above and four cases, Galerna creates two bulk jobs:

```text
bulk_0000 -> cases 0000, 0001
bulk_0001 -> cases 0002, 0003
```

Each bulk job asks for `threads: 2` and can run up to two case commands internally. Since `snakemake.cli.cores: 2`, Snakemake has only enough local budget to run one bulk job at a time:

```text
time 1: bulk_0000 uses 2 cores -> cases 0000 and 0001
time 2: bulk_0001 uses 2 cores -> cases 0002 and 0003
```

If you changed only `cores` to `4`, Snakemake could run two bulk jobs at the same time:

```text
time 1: bulk_0000 uses 2 cores -> cases 0000 and 0001
        bulk_0001 uses 2 cores -> cases 0002 and 0003
```

So, in local bulk mode:

```text
number of simultaneous bulk jobs ~= floor(cli.cores / rule.threads)
```

This matches the SLURM pattern where one submitted job reserves several cores and uses them to run several Galerna cases internally.

For a cluster-like setup such as:

```yaml
run:
  backend: snakemake
  mode: bulk
  executor: slurm
  cases_per_job: 32
  snakemake:
    rule:
      threads: 16
      resources:
        runtime: 10
        mem_mb_per_cpu: 1000
        slurm_partition: meteo_long
    cli:
      jobs: 20
```

the intended meaning is:

- each SLURM job handles 32 Galerna cases;
- inside that SLURM job, up to 16 case commands can run concurrently;
- at most 20 Snakemake/SLURM jobs are submitted or active at once.

With `executor: slurm`, Galerna delegates to Snakemake's SLURM executor.
`snakemake.rule.resources` is rendered in the generated Snakefile, while
`snakemake.cli` is passed to the `snakemake` command. If `snakemake.cli.jobs`
is omitted for SLURM, Galerna uses `--jobs unlimited`. This has to be run on a
system where SLURM and the Snakemake SLURM executor plugin are available.

Use `snakemake.rule.resources` for resources that belong to each generated
case or bulk rule. Use `snakemake.cli.default-resources` for Snakemake fallback
resources on rules that do not declare a value. Similarly,
`snakemake.rule.threads` is per generated rule, while `snakemake.cli.cores` is
the total local budget passed to Snakemake.

## Layouts

### Directory Layout

This is the default. Each case gets its own directory:

```text
output/
  0000/
    galerna.out
    galerna.err
    .galerna.done
  .galerna/
    cases.tsv
    status/
      status_0000.tsv
```

This is the best starting point when your model expects a working directory per case.

### Shared Layout

Use shared layout when inode count matters and you do not want one directory per case:

```yaml
cases:
  layout: shared
```

All commands run from `output_dir`, so commands must write unique output names:

```yaml
command: "python run_model.py --station {{ station }} --output result_{{ case_id }}.txt"
```

Shared layout keeps logs, status files, and technical done markers under `output/.galerna/`.

## Selecting Cases

Use `--cases` with comma-separated indices or ranges:

```bash
galerna build --cases 0-3
galerna run --cases 1,3
galerna status --cases 0,2-4
```

The manifest always contains all cases. `--cases` selects which cases to build, run, or show.

In Snakemake bulk mode, `--cases` must select complete groups. For example, with `cases_per_job: 2`, `--cases 0-1` is valid but `--cases 1` is rejected.

## Status

Galerna writes append-only status logs.

Reserved Galerna states include:

- `NOT_BUILT`: calculated by `galerna status` when a case is in the manifest but has no status line.
- `BUILT`: the case was generated by `build`.
- `STARTED`: execution started.
- `DONE`: execution completed successfully.
- `FAILED`: execution failed.

Users may append custom states such as `QC_OK`, `TRANSFERRED`, or `ARCHIVED` to status files.

Status history is stored under `output/.galerna/status/`. For directory layout,
logs remain in each case directory, but status files live outside the model run
directory. Large campaigns on shared filesystems can disable status history:

```yaml
status:
  mode: none
```

With `status.mode: none`, `galerna status` infers state from `cases.tsv` and
technical done markers.

Stdout and stderr logs can also be discarded independently:

```yaml
logs:
  stdout: discard
  stderr: file
```

Discarded streams are written to `/dev/null` in `cases.tsv`.

```bash
galerna status
galerna status --execution
```

`galerna status` reports the latest human status, including custom states. `galerna status --execution` reports the latest Galerna-reserved execution status.

## Custom Build Logic

For model-specific build logic, provide a wrapper class:

```yaml
wrapper:
  code: "custom_wrapper.py"
  class: "CustomWrapper"

templates_dir: "templates"
output_dir: "runs"

variable_parameters:
  station: [1, 2]

command: "python run_case.py"
```

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

`build_case(case_context)` runs after the case directory is created and before templates are rendered.

## Examples

The `examples/` folder contains executable learning paths:

| Example | Purpose |
| --- | --- |
| `directories/` | Start with one directory per case, then scale to Snakemake and SLURM |
| `shared/` | Start with one shared output directory, then scale to Snakemake and SLURM bulk |
| `advanced/custom_build_hook` | Wrapper inheritance with `build_case` |

Run any example from its own directory:

```bash
cd examples/directories/01_local_with_templates
galerna build
galerna run
galerna status
```

See [examples/README.md](examples/README.md) for the full list.

## More Details

See [docs/yaml_configuration_examples.md](docs/yaml_configuration_examples.md) for a longer YAML configuration guide, including SLURM and planned user-provided Snakefile interfaces.
