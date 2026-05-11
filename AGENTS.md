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

## Postprocess Design

Postprocess orchestration can be deferred, but agents should preserve a path for it.

Do not recommend folding postprocess work into the main per-case `command` as the default pattern. Keep `command` focused on running one simulation case. Future postprocess support should be modeled as an optional Galerna phase in the same YAML, not as a separate YAML that reuses the same case directories.

The preferred future shape is:

```yaml
postprocess:
  per_case:
    command: "python extract_metrics.py output.nc metrics_{{case_id}}.csv"
  aggregate:
    command: "python merge_metrics.py metrics_*.csv summary.csv"
  cleanup:
    policy: keep
```

Postprocess should reuse the same case manifest produced by build, so selected cases, layouts, and case IDs remain consistent across phases.

Postprocess should support:

- per-case extraction from outputs such as NetCDF files;
- optional aggregation across all selected cases into a single summary artifact;
- optional cleanup of heavy raw outputs only after successful postprocess.

Cleanup should be explicit and conservative. The default policy should keep raw outputs. Galerna, not the user's extraction script, should own cleanup policy when this feature is implemented.

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

For `cases.layout: directories`, per-case logs and status should live in each `case_dir`, for example:

- `galerna.out`
- `galerna.err`
- `galerna.status`
- `.galerna.done`

`galerna.status` is the human/historical status log. `.galerna.done` is the technical success marker for Snakemake and should only be created after the case command succeeds.

For `cases.layout: shared`, logs and status should live under `<output_dir>/.galerna/`, with status grouped by Snakemake job/bulk group where possible:

- `.galerna/logs/<case_id>.out`
- `.galerna/logs/<case_id>.err`
- `.galerna/status/status_<group_id>.tsv`
- `.galerna/done/<group_id>.done`

Use Snakemake input functions or `lambda` where needed so arbitrary Galerna case directory names are supported.
