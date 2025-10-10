#!/usr/bin/env python3
"""
Setup script for ac-core library.
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "AC Core - Signal insertion library for Alpha Crucible Quant framework"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="ac-core",
    version="0.1.0",
    author="Alpha Crucible Team",
    author_email="team@alphacrucible.com",
    description="Core library for inserting signals into Alpha Crucible Quant Supabase database",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/ac-core",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
    },
    keywords="quantitative finance, trading signals, database, supabase, portfolio optimization",
    project_urls={
        "Bug Reports": "https://github.com/your-org/ac-core/issues",
        "Source": "https://github.com/your-org/ac-core",
        "Documentation": "https://github.com/your-org/ac-core#readme",
    },
)
