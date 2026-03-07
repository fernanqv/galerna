from model_wrappers import ModelWrapper
import os

# Define template and output directories
templates_dir = "templates/xbeach_holland_default"
output_dir = "xbeach_holland_exp"

# Variable parameters (mapping thetamin to var1 and thetamax to var2 according to params.txt)
variable_parameters = {
    "var1": [225, 226, 227],  # These correspond to {{var1}} in the template
    "var2": [514, 315, 316]   # These correspond to {{var2}} in the template
}

# Fixed parameters (empty in this case)
fixed_parameters = {}

# Create a simple child class (although ModelWrapper is no longer abstract,
# it is common to inherit to define specific behaviors if needed)
class HollandWrapper(ModelWrapper):
    pass

# Instantiate the wrapper
wrapper = HollandWrapper(
    templates_dir=templates_dir,
    variable_parameters=variable_parameters,
    fixed_parameters=fixed_parameters,
    output_dir=output_dir,
    num_workers=2  # We can use parallelization even for the build
)

# Generate cases (build)
print("Generating cases in directory:", output_dir)
wrapper.build_cases(mode="all_combinations")
wrapper.run_cases(launcher="sbatch /nfs/home/geocean/valvanuz/model_wrappers/examples/launchers/slurm_xbeach.sh")
