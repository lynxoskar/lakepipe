#!/usr/bin/env python3
"""Update version to 1.0.0 and add PyYAML dependency"""

# Update lakepipe/__init__.py version
with open('lakepipe/__init__.py', 'r') as f:
    content = f.read()

content = content.replace('__version__ = "0.1.0"', '__version__ = "1.0.0"')

with open('lakepipe/__init__.py', 'w') as f:
    f.write(content)

# Update pyproject.toml to add PyYAML
with open('pyproject.toml', 'r') as f:
    content = f.read()

# Add PyYAML to dependencies (find the right spot)
content = content.replace(
    '    "orjson>=3.10.0",',
    '    "orjson>=3.10.0",\n    "pyyaml>=6.0.0",'
)

with open('pyproject.toml', 'w') as f:
    f.write(content)

print("✅ Updated version to 1.0.0 and added PyYAML dependency")
