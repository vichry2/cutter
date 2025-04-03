import os
import subprocess

def build_rs():
    # Get the path to the Rust crate
    rs_cutter_path = os.path.join(os.path.dirname(__file__), "rs_cutter")

    # Get the path to the Poetry environment
    poetry_env_path = subprocess.run(
        ["poetry", "env", "info", "--path"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    # Set environment variables to use Poetry's Python
    env = os.environ.copy()
    env["PATH"] = f"{poetry_env_path}/bin:" + env["PATH"]
    env["VIRTUAL_ENV"] = poetry_env_path
    env.pop("CONDA_PREFIX", None)
    print("Building Rust extension using maturin...")

    # Run maturin develop with the correct environment
    result = subprocess.run(
        ["maturin", "develop", "--release"],
        cwd=rs_cutter_path,
        check=True,
        env=env,
    )

    if result.returncode != 0:
        raise RuntimeError("Failed to build Rust extension")
    
