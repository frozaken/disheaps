#!/usr/bin/env python3
"""
Script to generate Python protobuf code from proto definitions.

This script uses the protoc compiler to generate Python gRPC client code
from the disheap.proto file.
"""

import os
import subprocess
import sys
from pathlib import Path


def run_protoc():
    """Run protoc to generate Python gRPC code."""
    
    # Get project root directory
    project_root = Path(__file__).parent.parent.parent
    proto_dir = project_root / "proto"
    output_dir = project_root / "disheap-python" / "disheap" / "_generated"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create __init__.py in generated directory
    init_file = output_dir / "__init__.py"
    if not init_file.exists():
        init_file.write_text('"""Generated protobuf code for Disheap."""\n')
    
    # protoc command
    cmd = [
        "python", "-m", "grpc_tools.protoc",
        f"--proto_path={proto_dir}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        f"--pyi_out={output_dir}",  # Generate type stubs
        str(proto_dir / "disheap.proto")
    ]
    
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Protobuf generation successful!")
        
        # Fix imports in generated files (common issue with protoc)
        fix_imports(output_dir)
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error running protoc: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: protoc not found. Please install grpcio-tools:")
        print("  pip install grpcio-tools")
        return False


def fix_imports(output_dir: Path):
    """Fix import statements in generated files."""
    
    # Find all _pb2_grpc.py files
    for grpc_file in output_dir.glob("*_pb2_grpc.py"):
        content = grpc_file.read_text()
        
        # Replace relative imports with absolute imports
        # This is a common issue with protoc generation
        if "import disheap_pb2 as disheap__pb2" in content:
            content = content.replace(
                "import disheap_pb2 as disheap__pb2",
                "from . import disheap_pb2 as disheap__pb2"
            )
            grpc_file.write_text(content)
            print(f"Fixed imports in {grpc_file.name}")


if __name__ == "__main__":
    if not run_protoc():
        sys.exit(1)
