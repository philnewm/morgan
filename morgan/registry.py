"""
Package registry functionality for target package registries.

This module provides abstractions for working with different target package registries,
for mirroring.
"""

import json
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
        self.headers = {}
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"
        self._packages_cache: Optional[list] = None
        self._package_files_cache: dict[int, list] = {}

    @property
    def name(self) -> str:
        return f"GitLab ({self.registry_url})"

    def _fetch_packages_list(self) -> list:
        """Fetch package files from the API and cache the result."""
        if self._packages_cache is not None:
            return self._packages_cache

        api_url = f"{self.registry_url}/api/v4/projects/{self.project}/packages"

        try:
            request = urllib.request.Request(api_url, headers=self.headers)
            with urllib.request.urlopen(request) as response:
                package_list = json.load(response)
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to fetch package files from {api_url}") from e

        if not isinstance(package_list, list):
            raise RuntimeError(
                f"Unexpected response format from {self.name}, expected a list: {package_list}"
            )

        self._packages_cache = package_list
        return package_list

    def _fetch_package_files(self, package_id: int) -> list:
        """Fetch package files from the API and cache the result."""
        if (
            package_id in self._package_files_cache
            and self._package_files_cache[package_id] is not None
        ):
            return self._package_files_cache[package_id]

        api_url = f"{self.registry_url}/api/v4/projects/{self.project}/packages/{package_id}/package_files"

        try:
            request = urllib.request.Request(api_url, headers=self.headers)
            with urllib.request.urlopen(request) as response:
                package_file_list = json.load(response)
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to fetch package files from {api_url}") from e

        if not isinstance(package_file_list, list):
            raise RuntimeError(
                f"Unexpected response format from {self.name}, expected a list: {package_file_list}"
            )

        self._package_files_cache[package_id] = package_file_list
        return self._package_files_cache[package_id]

    def has_package(
        self,
        file_name: str,
        package_name: str,
        hash_alg: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        if expected_hash:
            print("Warning: hash verification is not implemented yet")

        packages = self._fetch_packages_list()

        for package in packages:
            if package.get("name") != package_name:
                continue

            for file_info in self._fetch_package_files(package.get("id")):
                if file_info.get("file_name") != file_name:
                    continue

                if file_info.get(f"file_{hash_alg}") == expected_hash:
                    return True
                else:
                    raise RuntimeError(
                        f"Hash of {file_name} does not match, unknown how to proceed"
                    )

        return False

    def clear_cache(self):
        """Clear the cached package files. Useful for testing or if registry content changes."""
        self._packages_cache = None
