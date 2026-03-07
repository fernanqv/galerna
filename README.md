# Model Wrappers

**This project is a minimal fork of [BlueMath_tk](https://github.com/GeoOcean/BlueMath_tk).**

Minimal infrastructure for managing and running numerical model cases. This project provides a base `ModelWrapper` class to handle templating, parameter management, and parallel execution of numerical models.

## Installation

This package requires Python 3.11+ and `jinja2`.

```bash
pip install -e .
```

## Usage

The main workflow involves creating a custom wrapper class that inherits from `ModelWrapper`, defining your parameters, and then building and running the cases.

### Example: Holland Model

Here is how to use the wrapper for a Holland model, based on the `examples/example_holland.py` script.

```python
from model_wrappers import ModelWrapper
import os

# 1. Define template and output directories
templates_dir = "templates/xbeach_holland_default"
output_dir = "xbeach_holland_exp"

# 2. Define Variable parameters
# Each key corresponds to a placeholder in your template files (e.g., {{var1}})
variable_parameters = {
    "var1": [225, 226, 227],
    "var2": [514, 315, 316]
}

# 3. Define Fixed parameters (optional)
fixed_parameters = {}

# 4. Create a custom wrapper class
class HollandWrapper(ModelWrapper):
    pass

# 5. Instantiate the wrapper
wrapper = HollandWrapper(
    templates_dir=templates_dir,
    variable_parameters=variable_parameters,
    fixed_parameters=fixed_parameters,
    output_dir=output_dir,
    num_workers=2  # Number of parallel processes
)

# 6. Generate cases (rendering templates)
# mode="all_combinations" will generate all possible combinations of variable parameters
# mode="one_by_one" will pair parameters by index
wrapper.build_cases(mode="all_combinations")

# 7. Run cases using a launcher command (optional)
launcher_cmd = "sbatch /path/to/your/launcher.sh"
wrapper.run_cases(launcher=launcher_cmd)
```

## Key Features

- **Jinja2 Templating**: Easily inject parameters into model input files.
- **Parallel Execution**: efficient case building and running using `concurrent.futures`.
- **Flexible Parameters**: Support for exhaustive combinations or specific case pairs.
- **Launcher Support**: Seamless integration with Slurm or local shell scripts.
