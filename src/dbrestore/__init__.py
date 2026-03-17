"""This package holds the whole dbrestore application.
Most of the real flow lives in config, operations, storage, and the adapter layer.
CLI and GUI are only entry points on top of those shared services.
If you need to re-understand the project, start from operations.py and branch out from there."""

__all__ = ["__version__"]

__version__ = "0.1.0"
