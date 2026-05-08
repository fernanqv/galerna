# Galerna Agent Guidelines

These guidelines apply to agents working in this repository.

## Python Quality

- Use `ruff` for Python changes.
- Do not introduce new Ruff violations.
- Prefer small, scoped changes that follow the current codebase style.
- Do not refactor unrelated code unless explicitly requested.

## Execution Architecture

Galerna should keep `command` as the per-case command. It is rendered with each case context and executed from that case directory.

The first execution architecture should support:

- `run.backend: local`: Galerna executes cases sequentially. No parallel execution.
- `run.backend: snakemake`: Galerna generates Snakemake artifacts and delegates orchestration.

Snakemake execution should support:

- `run.mode: cases`: one Snakemake task per case.
- `run.mode: bulk`: grouped cases per task/job.
- `run.executor: local`: Snakemake runs locally.
- `run.executor: slurm`: Snakemake submits to SLURM.

Galerna should not grow a full custom scheduler. It should build and describe parametric cases; Snakemake should handle serious orchestration.

## YAML Interface

Keep the user-facing YAML simple. Do not expose Snakemake complexity unless the user explicitly needs it.

Preferred simple example:

```yaml
command: "python run_model.py {{station}}"

run:
  backend: snakemake
  mode: cases
  executor: local
  cores: 4
```

For direct local debugging:

```yaml
run:
  backend: local
```

This must mean sequential execution only.

## Snakemake Artifacts

Galerna may generate files under `<output_dir>/.galerna/`, such as:

- `cases.tsv`
- `Snakefile`

Per-case logs should live in each `case_dir` by default, for example:

- `galerna.out`
- `galerna.err`
- `.galerna.done`

Use Snakemake input functions or `lambda` where needed so arbitrary Galerna case directory names are supported.

