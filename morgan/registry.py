"""
Package registry functionality for target package registries.

This module provides abstractions for working with different target package registries,
for mirroring.
"""

import os
import urllib.error
import urllib.request
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


class GitLabRegistry(Registry):
    """Registry implementation that checks a GitLab package registry."""

    def __init__(self, registry_url: str, project: str, token: Optional[str] = None):
        self.registry_url = registry_url
        self.project = project
        self.token = token
        self._package_files_cache: Optional[list]

    @property
    def name(self) -> str:
        return f"GitLab ({self.registry_url})"

    def _fetch_package_files(self) -> list:
        """Fetch package files from the API and cache the result."""
        if self._package_files_cache is not None:
            return self._package_files_cache

        # Construct the API URL to query the package
        api_url = (
            f"{self.registry_url}/api/v4/projects/{self.project}/packages/pypi/files"
        )

        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        try:
            request = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(request) as response:
                package_list = json.load(response)
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to fetch package files from {self.name}") from e

        if not isinstance(package_list, list):
            raise RuntimeError(
                f"Unexpected response format from {self.name}, expected a list: {package_list}"
            )

        self._package_files_cache = package_list
        return package_list

    def has_package(
        self,
        file_name: str,
        package_name: str,
        hash_alg: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        if expected_hash:
            print("Warning: hash verification is not implemented yet")

        package_files = self._fetch_package_files()

        # Look for the specific file
        for file_info in package_files:
            if file_info.get("file_name") == file_name:
                return True
                # TODO: Implement hash checking using "https://gitlab.example.com/api/v4/projects/:id/packages/:package_id/package_files"

        return False

    def clear_cache(self):
        """Clear the cached package files. Useful for testing or if registry content changes."""
        self._package_files_cache = None
