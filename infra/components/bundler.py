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
