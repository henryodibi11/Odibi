from setuptools import setup

# Modules to compile
# We only compile the core control plane modules that are stable and frequently used.
modules = [
    "odibi/graph.py",
    "odibi/pipeline.py",
    "odibi/config.py",
    "odibi/context.py",
    "odibi/state.py",
]

# Check if we are in a build environment that supports compilation
# If mypy is not installed, or we explicitly disable it, we skip compilation.
# This allows 'pip install -e .' to work without compiling during dev.
try:
    from mypyc.build import mypycify

    ext_modules = mypycify(modules)
except (ImportError, RuntimeError):
    # Fallback to pure Python if mypyc is not present or fails
    ext_modules = []

setup(
    name="odibi",
    ext_modules=ext_modules,
)
