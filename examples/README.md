# Galerna Local Backend Examples

These examples exercise the first clean Galerna execution path:

```yaml
run:
  backend: local
```

Local execution is sequential. It is intended for debugging and simple runs, not for parallel execution.

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

This example documents the intended YAML interface. It will run once `wrapper:` support is implemented in the CLI.

### 05 Failure Status

One case succeeds and one fails. This is useful for checking `galerna.status` and `.galerna.done` behavior.

```bash
cd examples/05_failure_status
galerna run
```

The command exits with an error by design.
