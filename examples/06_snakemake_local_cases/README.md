# Snakemake Local Cases Example

This example uses Galerna to build four cases and Snakemake to run one task per case locally.

```yaml
run:
  backend: snakemake
  mode: cases
  executor: local
  cores: 2
```

Run it from this folder:

```bash
galerna build
galerna run
galerna status
```

`cores: 2` lets Snakemake run up to two case tasks at the same time.

Useful files after running:

```text
runs/.galerna/cases.tsv
runs/.galerna/Snakefile
runs/case_0000/galerna.out
runs/case_0000/galerna.err
runs/case_0000/galerna.status
runs/case_0000/.galerna.done
runs/case_0000/result_case_0000.txt
```

Run a subset:

```bash
galerna run --cases 1,3
galerna status
```

Show the latest Galerna execution status, ignoring custom user statuses:

```bash
galerna status --execution
```
