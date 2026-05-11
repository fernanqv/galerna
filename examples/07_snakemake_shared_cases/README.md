# Snakemake Shared Cases Example

This example uses Galerna with a shared case layout and Snakemake local execution.

```yaml
cases:
  layout: shared

run:
  backend: snakemake
  mode: cases
  executor: local
  cores: 2
```

Use this layout when you want to reduce inode usage by avoiding one directory per case.

Run it from this folder:

```bash
galerna build
galerna run
galerna status
```

The command writes case-specific outputs directly under `runs/`:

```text
runs/result_case_0000.txt
runs/result_case_0001.txt
```

Because all cases run in the same directory, commands must include `{{ case_id }}` or another unique value in their output names. Otherwise cases can overwrite each other.

Snakemake artifacts and Galerna logs live under `runs/.galerna/`:

```text
runs/.galerna/cases.tsv
runs/.galerna/Snakefile
runs/.galerna/logs/case_0000.out
runs/.galerna/logs/case_0000.err
runs/.galerna/status/status_cases.tsv
runs/.galerna/done/case_0000.done
```

The status file is grouped:

```text
runs/.galerna/status/status_cases.tsv
```

but technical done markers are per case:

```text
runs/.galerna/done/case_0000.done
runs/.galerna/done/case_0001.done
```

Snakemake needs one distinct output marker per case rule.

Run a subset:

```bash
galerna run --cases 1,3
galerna status
```
