import os
from galerna.base import Galerna

def main():
    # We define the runner strictly using the Object Oriented approach 
    # instead of passing a YAML file and relying on the CLI.
    wrapper = Galerna(
        templates_dir="templates",
        variable_parameters={"station": list(range(1, 16))},
        output_dir="output_array",
        command="python dummy_script.py {{station}}",
        log_level="DEBUG",
    )
    
    print("Building cases...")
    wrapper.build_cases()
    wrapper.run_cases()

if __name__ == "__main__":
    main()
