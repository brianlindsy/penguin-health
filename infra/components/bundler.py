"""
Lambda bundling utilities for Python Lambda functions.
"""

import shutil
import jsii
from aws_cdk import ILocalBundling


@jsii.implements(ILocalBundling)
class CopyFileBundler:
    """Bundles a single Python file into the Lambda asset output directory."""

    def __init__(self, source_path: str):
        self._source_path = source_path

    def try_bundle(self, output_dir: str, options) -> bool:
        shutil.copy2(self._source_path, output_dir)
        return True


@jsii.implements(ILocalBundling)
class MultiFileBundler:
    """Bundles multiple Python files into the Lambda asset output directory."""

    def __init__(self, source_paths: list[str]):
        self._source_paths = source_paths

    def try_bundle(self, output_dir: str, options) -> bool:
        for path in self._source_paths:
            shutil.copy2(path, output_dir)
        return True


@jsii.implements(ILocalBundling)
class DirectoryBundler:
    """
    Bundles files and directories into the Lambda asset output directory.

    Args:
        items: List of tuples (source_path, dest_name) where:
            - source_path: Path to file or directory
            - dest_name: Name in output (None to use original name)
    """

    def __init__(self, items: list[tuple[str, str | None]]):
        self._items = items

    def try_bundle(self, output_dir: str, options) -> bool:
        import os
        for source_path, dest_name in self._items:
            dest = os.path.join(output_dir, dest_name or os.path.basename(source_path))
            if os.path.isdir(source_path):
                shutil.copytree(source_path, dest)
            else:
                shutil.copy2(source_path, dest)
        return True
