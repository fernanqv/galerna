import os
from model_wrappers import ModelWrapper

def test_templates_dir_existence():
    non_existent_dir = "non_existent_templates_dir"
    if os.path.exists(non_existent_dir):
        if os.path.isdir(non_existent_dir):
            os.rmdir(non_existent_dir)
        else:
            os.remove(non_existent_dir)
    
    variable_parameters = {"param1": [1]}
    fixed_parameters = {}
    output_dir = "test_output"
    
    # Verify that FileNotFoundError is raised
    error_caught = False
    try:
        wrapper = ModelWrapper(
            templates_dir=non_existent_dir,
            variable_parameters=variable_parameters,
            fixed_parameters=fixed_parameters,
            output_dir=output_dir
        )
    except FileNotFoundError as e:
        print(f"Caught expected error: {e}")
        assert non_existent_dir in str(e)
        error_caught = True
    
    if not error_caught:
        raise AssertionError("Should have raised FileNotFoundError")
        
    print("Validation successful!")

if __name__ == "__main__":
    test_templates_dir_existence()
