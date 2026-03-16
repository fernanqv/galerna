import argparse
import json
import importlib.util
import os
import sys
from typing import Type
from galerna.base import Galerna

def load_custom_wrapper(file_path: str, class_name: str = "CustomGalerna") -> Type[Galerna]:
    """
    Dynamically loads a Galerna subclass from a .py file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Custom wrapper file not found: {file_path}")
    
    spec = importlib.util.spec_from_file_location("custom_wrapper", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {file_path}")
    
    module = importlib.util.module_from_spec(spec)
    sys.modules["custom_wrapper"] = module
    spec.loader.exec_module(module)
    
    # Try to find a subclass of Galerna if class_name is default
    if class_name == "CustomGalerna":
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and 
                issubclass(attr, Galerna) and 
                attr is not Galerna):
                return attr
    
    try:
        return getattr(module, class_name)
    except AttributeError:
        raise AttributeError(f"Module {file_path} has no class {class_name}")

def main():
    parser = argparse.ArgumentParser(description="CLI for building and running model wrappers.")
    parser.add_argument("action", choices=["build", "run", "both"], help="Action to perform.")
    parser.add_argument("--config", required=True, help="Path to the JSON configuration file.")
    parser.add_argument("--launcher", help="Override the launcher command.")
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    # Extract wrapper configuration
    wrapper_code_path = config.get("wrapper_code")
    wrapper_class_name = config.get("wrapper_class", "CustomGalerna")
    
    if wrapper_code_path:
        print(f"Loading custom wrapper from {wrapper_code_path}...")
        WrapperClass = load_custom_wrapper(wrapper_code_path, wrapper_class_name)
    else:
        WrapperClass = Galerna
        
    # Instantiate the wrapper
    # Remove CLI-specific keys from config to pass as kwargs
    wrapper_params = config.copy()
    for key in ["wrapper_code", "wrapper_class", "launcher"]:
        wrapper_params.pop(key, None)
        
    wrapper = WrapperClass(**wrapper_params)
    
    if args.action in ["build", "both"]:
        print("Building cases...")
        wrapper.build_cases()
        
    if args.action in ["run", "both"]:
        launcher = args.launcher or config.get("launcher")
        if not launcher:
            print("Error: Launcher not specified in CLI or config JSON.")
            sys.exit(1)
        print(f"Running cases with launcher: {launcher}")
        wrapper.run_cases(launcher=launcher)

if __name__ == "__main__":
    main()
