#!/usr/bin/env uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click>=8.1.8",
#     "tomlkit>=0.13.2"
# ]
# ///
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import click
import json
import logging
import re
import sys
import tomlkit
from dataclasses import dataclass
from pathlib import Path
from typing import NewType, Protocol


# Configure logging to stderr
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s', stream=sys.stderr)

Version = NewType('Version', str)

# https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
SemVerRegEx = r'^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$'


class Package(Protocol):
    """The package protocol."""

    path: Path

    def package_name(self) -> str:
        """The package name."""
        ...

    def package_version(self) -> str:
        """The package version."""
        ...

    def bump_version(self) -> str:
        """Update the package version."""
        ...


@dataclass
class NpmPackage:
    """A NPM package."""

    path: Path

    def package_name(self) -> str:
        """Get the package name from the package.json file."""
        with open(self.path / 'package.json', 'r', encoding='utf-8') as f:
            return json.load(f)['name']

    def package_version(self) -> str:
        """Get the package version from the package.json file."""
        with open(self.path / 'package.json', 'r', encoding='utf-8') as f:
            return json.load(f)['version']

    def bump_version(self) -> str:
        """Update the package.json with a version."""
        with open(self.path / 'package.json', 'r+', encoding='utf-8') as f:
            data = json.load(f)
            matched = re.match(SemVerRegEx, data['version'])
            patch = int(matched.group('patch')) + 1
            if patch > sys.maxsize:
                patch = 0
            version = '.'.join([matched.group('major'), matched.group('minor'), str(patch)])
            data['version'] = version
            f.seek(0)
            json.dump(data, f, indent=2)
            f.truncate()
            return version


@dataclass
class PyPiPackage:
    """A PyPi package."""

    path: Path

    def package_name(self) -> str:
        """Get the package name from the pyproject.toml file."""
        with open(self.path / 'pyproject.toml', encoding='utf-8') as f:
            toml_data = tomlkit.parse(f.read())
            name = toml_data.get('project', {}).get('name')
            if not name:
                raise ValueError('No name in pyproject.toml project section')
            return str(name)

    def package_version(self) -> str:
        """Read the version from the pyproject.toml file."""
        with open(self.path / 'pyproject.toml', encoding='utf-8') as f:
            toml_data = tomlkit.parse(f.read())
            version = toml_data.get('project', {}).get('version')
            if not version:
                raise ValueError('No version in pyproject.toml project section')
            return str(version)

    def bump_version(self) -> str:
        """Update version in pyproject.toml."""
        package_name = self.package_name()
        version_str = self.package_version()
        with open(self.path / 'pyproject.toml', encoding='utf-8') as f:
            data = tomlkit.parse(f.read())
            # Access the version safely from tomlkit document
            project_table = data.get('project')
            if project_table is None:
                raise ValueError('No project section in pyproject.toml')
            matched = re.match(SemVerRegEx, version_str)
            patch = int(matched.group('patch')) + 1
            if patch > sys.maxsize:
                patch = 0
            version = '.'.join([matched.group('major'), matched.group('minor'), str(patch)])

            # Update the version safely
            project_table['version'] = version

        with open(self.path / 'pyproject.toml', 'w', encoding='utf-8') as f:
            f.write(tomlkit.dumps(data))

        # Find the corresponding __init__.py file
        # Convert package name from awslabs.package-name to package_name format
        if package_name.startswith('awslabs.'):
            # Remove 'awslabs.' prefix and convert hyphens
            module_name = package_name[8:].replace('-', '_')
            init_file = self.path / 'awslabs' / module_name / '__init__.py'
            if init_file.exists():
                # Read current __init__.py
                with open(init_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Update __version__ line
                version_pattern = r"__version__\s*=\s*['\"][^'\"]*['\"]"
                new_version_line = f"__version__ = '{version}'"
                if re.search(version_pattern, content):
                    # Replace existing __version__
                    updated_content = re.sub(version_pattern, new_version_line, content)
                    with open(init_file, 'w', encoding='utf-8') as f:
                        f.write(updated_content)
                    click.echo(f"Updated {init_file}: __version__ = '{version}'")
                else:
                    click.echo(f'Warning: No __version__ found in {init_file}')
            else:
                click.echo(f'Warning: {init_file} not found for package {package_name}')
        else:
            click.echo(
                f"Warning: Package {package_name} doesn't follow awslabs.* naming convention"
            )
        return version


@click.group()
def cli():
    """Simply pass."""
    pass


@cli.command('bump-package')
@click.option('--directory', type=click.Path(exists=True, path_type=Path), default=Path.cwd())
def bump_package(directory: Path) -> int:
    """Updates the package version with a patch."""
    # Detect package type
    if Path(str(directory), 'pyproject.toml').is_file():
        logging.debug('Found PyPI package at %s', directory)
        package = PyPiPackage(directory)
        name = package.package_name()
        version = package.bump_version()
        click.echo(f'{name}@{version}')
    if Path(str(directory), 'package.json').is_file():
        logging.debug('Found NPM package at %s', directory)
        package = NpmPackage(directory)
        name = package.package_name()
        version = package.bump_version()
        click.echo(f'{name}@{version}')

    return 0


if __name__ == '__main__':
    sys.exit(cli())
