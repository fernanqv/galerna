import os
from galerna import Galerna

def test_templates_dir_existence():
    non_existent_dir = "non_existent_templates_dir"
    if os.path.exists(non_existent_dir):
        if os.path.isdir(non_existent_dir):
            os.rmdir(non_existent_dir)
        else:
            os.remove(templates_dir)
    
    variable_parameters = {"param1": [1]}
    fixed_parameters = {}
    output_dir = "test_output"
    
    # Verify that OSError is raised with pytest.raises
    with pytest.raises(OSError, match=f"Templates directory '{templates_dir}' doesn't exist."):
        Galerna(
            templates_dir=templates_dir,
            output_dir=output_dir,
            variable_parameters=variable_parameters, # Added back
            fixed_parameters=fixed_parameters,       # Added back
            cases_name_format="case_{param1}"        # Added, assuming a simple format
        )
    if not error_caught:
        raise AssertionError("Should have raised FileNotFoundError")
        
    print("Validation successful!")

if __name__ == "__main__":
    test_templates_dir_existence()
