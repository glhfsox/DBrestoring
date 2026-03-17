"""This is the simplest executable entry point for the package.
Python reaches this file when the app is started with ``python -m dbrestore``.
It does not contain business logic and only forwards control to the CLI module.
That keeps one real command surface instead of splitting behavior across files."""

from dbrestore.cli import main


if __name__ == "__main__":
    main()
