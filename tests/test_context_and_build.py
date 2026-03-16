import os
import shutil
from galerna import Galerna

def test_context_and_build():
    output_dir = "test_output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    variable_parameters = {
        "param1": [1, 2],
        "param2": ["a", "b"]
    }
    fixed_parameters = {"fixed": 100}

    wrapper = Galerna(
        templates_dir=None,
        variable_parameters=variable_parameters,
        fixed_parameters=fixed_parameters,
        output_dir=output_dir
    )

    # Test load_cases and get_context
    print("Testing get_context...")
    ctx = wrapper.get_context()
    print(f"Context type: {type(ctx)}")
    print(f"Context content:\n{ctx}")

    # Test build_cases with subset and string format
    print("\nTesting build_cases with subset [0] and string format...")
    wrapper.build_cases(mode="all_combinations", cases=[0], cases_name_format="p1_{param1}_p2_{param2}")
    
    built_dirs = os.listdir(output_dir)
    print(f"Built directories: {built_dirs}")
    assert "p1_1_p2_a" in built_dirs
    
    # Test instance level naming
    print("\nTesting instance level naming...")
    wrapper.cases_name_format = "instance_{case_num}"
    wrapper.build_cases(mode="all_combinations", cases=[1])
    built_dirs = os.listdir(output_dir)
    print(f"Built directories: {built_dirs}")
    assert "instance_1" in built_dirs
    
    print("\nVerification successful!")

if __name__ == "__main__":
    test_context_and_build()
