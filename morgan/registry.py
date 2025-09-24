"""
Package registry functionality for target package registries.

This module provides abstractions for working with different target package registries,
for mirroring.
"""

import os
from abc import ABC, abstractmethod
from typing import Callable, Optional


class Registry(ABC):
    """Abstract base class for package registries."""

    @abstractmethod
    def has_package(
        self,
        file_name: str,
        package_name: str,
        hash_alg: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        """
        Check if package exists in the registry and download it if available.

        Args:
            fileinfo: Information about the file to download
            target: Target path where to save the file
            hashalg: Hash algorithm to use for verification
            hash_file_func: Function to calculate file hash

        Returns:
            True if file was successfully retrieved, False otherwise
        """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this registry."""


class LocalRegistry(Registry):
    """Registry implementation that checks the local file system."""

    def __init__(self, hash_file_func: Callable, index_path: str):
        self.hash_file_func = hash_file_func
        self.index_path = index_path

    @property
    def name(self) -> str:
        return "Local"

    def has_package(
        self,
        file_name: str,
        package_name: str,
        hash_alg: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        # if target already exists, verify its hash and only download if
        # there's a mismatch
        target = os.path.join(self.index_path, package_name, file_name)
        if not os.path.exists(target):
            return False

        # nothing else to do if there is no expected hash
        if not expected_hash:
            return True

        truehash = self.hash_file_func(target, hash_alg)
        if truehash != expected_hash:
            return False

        return True
