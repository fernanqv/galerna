This example shows how to load variable values from a CSV file.

Steps:

- `stations.csv` contains one station id per line.
- `galerna.yaml` uses `station: "file:stations.csv"` to load those values.

Run:

```bash
cd examples/directories/06_file_parameters
galerna build
galerna run
galerna status
```
