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

    @staticmethod
    def _parse_link_header(link_header: str) -> dict[str, str]:
        """Parse the Link header according to RFC2068 section 19.6.2.4.

        Args:
            link_header: The Link header string to parse.

        Returns:
            A dictionary mapping relationship types to URLs.
        """
        if not link_header:
            return {}

        links = {}

        # Process each link section (separated by commas)
        for section in link_header.split(","):
            section = section.strip()
            if not section:
                continue  # Skip empty sections

            # Find the URL part first (should be at the start, enclosed in angle brackets)
            parts = section.split(";")
            if not parts:
                continue  # Invalid format

            url_part = parts[0].strip()
            if not (url_part.startswith("<") and url_part.endswith(">")):
                print(
                    f"Error: Unexpected Link header format: {url_part}. Ignoring section."
                )
                continue  # Invalid format

            url = url_part[1:-1]  # Remove <,> brackets

            # Process the parameters
            for param in parts[1:]:
                param = param.strip()
                if "=" not in param:
                    print(
                        f"Error: Unable to find '=' in link header: {param}. Ignoring parameter."
                    )
                    continue

                name, value = param.split("=", 1)
                name = name.strip().lower()  # Normalize to lowercase
                value = value.strip()

                if name != "rel":
                    continue  # We only care about 'rel' parameters

                # If value is quoted, it may contain multiple space-separated relation types
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]  # Remove quotes

                    # Handle multiple relation types in a quoted string
                    for rel_type in value.split():
                        links[rel_type.lower()] = url  # Normalize to lowercase
                else:
                    # Otherwise, it's a single relation type
                    links[value.lower()] = url  # Normalize to lowercase

        return links

    def _fetch_paginated_api(self, api_url: str) -> list:
        """Fetch paginated results from GitLab API.

        Args:
            api_url: The API URL to fetch from, including any query parameters

        Returns:
            List of all items from all pages

        Raises:
            RuntimeError: If the API request fails or returns unexpected format
        """
        results = []
        current_url: Optional[str] = api_url + "?per_page=100"

        while current_url:
            try:
                request = urllib.request.Request(current_url, headers=self.headers)
                with urllib.request.urlopen(request) as response:
                    page_data = json.load(response)

                    # Check response format
                    if not isinstance(page_data, list):
                        raise RuntimeError(
                            f"Unexpected response format from {self.name}, expected a list: {page_data}"
                        )

                    results.extend(page_data)

                    # Check for pagination in Link header
                    link_header = response.headers.get("Link")
                    if not link_header:
                        break

                    links = self._parse_link_header(link_header)
                    current_url = links.get("next")

            except (urllib.error.URLError, json.JSONDecodeError) as e:
                raise RuntimeError(f"Failed to read data from {current_url}") from e

        return results

    def _fetch_packages_list(self) -> list:
        """Fetch package files from the API and cache the result."""
        if self._packages_cache is not None:
            return self._packages_cache

        api_url = f"{self.registry_url}/api/v4/projects/{self.project}/packages"
        self._packages_cache = self._fetch_paginated_api(api_url)
        return self._packages_cache

    def _fetch_package_files(self, package_id: int) -> list:
        """Fetch package files from the API and cache the result."""
        if (
            package_id in self._package_files_cache
            and self._package_files_cache[package_id] is not None
        ):
            return self._package_files_cache[package_id]

        api_url = f"{self.registry_url}/api/v4/projects/{self.project}/packages/{package_id}/package_files"
        self._package_files_cache[package_id] = self._fetch_paginated_api(api_url)
        return self._package_files_cache[package_id]

    def has_package(
        self,
        file_name: str,
        package_name: str,
        hash_alg: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
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
        self._package_files_cache = {}
