# Galerna Examples

These examples exercise Galerna's first clean execution paths:

```yaml
run:
  backend: local
```

and:

```yaml
run:
  backend: snakemake
  mode: cases
  executor: local
```

Direct local execution is sequential. Snakemake local execution can run multiple case tasks concurrently using `run.cores`.

Run any example from its own folder:

```bash
cd examples/01_template_model
galerna build
galerna run
```

Useful files after a run:

```text
runs/.galerna/cases.tsv
runs/<case_id>/galerna.out
runs/<case_id>/galerna.err
runs/<case_id>/galerna.status
runs/<case_id>/.galerna.done
```

`galerna.status` is the human-readable history. `.galerna.done` is the technical success marker.

## Examples

### 01 Template Model

The most common layout: one directory per case, with templates rendered into each case directory.

```bash
cd examples/01_template_model
galerna build
galerna run
```

Try a subset:

```bash
galerna run --cases 1,3
```

### 02 Directories Without Templates

One directory per case, no templates. The command itself writes a case-specific result.

```bash
cd examples/02_directories_no_templates
galerna run
```

### 03 Shared Layout Without Templates

All cases run in the same `runs/` directory. This avoids one directory per case and is useful for filesystems with strict inode limits.

The command must use `case_id` to avoid overwriting outputs.

```bash
cd examples/03_shared_no_templates
galerna run
```

### 04 Custom Build Hook

Uses `wrapper.code` and `wrapper.class` to add custom Python logic in `build_case`.

```bash
cd examples/04_custom_build_hook
galerna run
```

### 05 Failure Status

One case succeeds and one fails. This is useful for checking `galerna.status` and `.galerna.done` behavior.

```bash
cd examples/05_failure_status
galerna run
```

The command exits with an error by design.

### 06 Snakemake Local Cases

Uses `run.backend: snakemake` with one Snakemake task per Galerna case.

```bash
cd examples/06_snakemake_local_cases
galerna build
galerna run
galerna status
```

This example generates `runs/.galerna/Snakefile` and runs up to two cases at the same time with `cores: 2`.

### 07 Snakemake Shared Cases

Uses `cases.layout: shared` with `run.backend: snakemake`. This avoids one directory per case and keeps status grouped under `runs/.galerna/status/`.

```bash
cd examples/07_snakemake_shared_cases
galerna build
galerna run
galerna status
```

Outputs must include `case_id` or another unique value because all cases run from the same `runs/` directory.
