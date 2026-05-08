# Manual Snakemake Example

Run from the generated Galerna metadata directory:

```bash
cd examples/snakemake_manual/runs/.galerna
snakemake --cores 2
```

Each case writes its logs in its own case directory:

```text
runs/case_0000/galerna.out
runs/case_0000/galerna.err
runs/case_0000/.galerna.done
```

## Run With SLURM

Snakemake 9 uses a separate SLURM executor plugin. Make sure the environment
has `snakemake-executor-plugin-slurm` installed.

From a login node with access to `sbatch`:

```bash
cd examples/snakemake_manual/runs/.galerna
snakemake --profile profiles/slurm
```

The profile submits jobs to the `meteo_long` partition.

To run only selected cases, pass their `.done` files as targets:

```bash
snakemake --profile profiles/slurm ../case_0001/.galerna.done ../case_0003/.galerna.done
```
