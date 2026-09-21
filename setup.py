"""
Minimal setup.py so `pip install -e .` makes src/ importable as top-level packages.
"""
from setuptools import setup, find_packages

setup(
    name="livestock_outbreak_detection",
    version="1.1.0",
    description="Livestock outbreak detection system",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
)