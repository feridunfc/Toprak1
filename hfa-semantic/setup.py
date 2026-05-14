"""
hfa-semantic/setup.py

IRONCLAD Semantic Layer — Setup script
"""

from setuptools import find_packages, setup

setup(
    name="hfa-semantic",
    version="0.1.0",
    description="IRONCLAD semantic intelligence layer — distributed truth engine",
    author="IRONCLAD Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "redis>=5.0",
        "pydantic>=2.0",
        "prometheus-client>=0.20",
    ],
    extras_require={
        "dev": [
            "pytest>=8",
            "pytest-asyncio>=0.23",
            "pytest-mock>=3.14",
            "fakeredis[aioredis]>=2.23",
            "anyio>=4.0",
        ]
    },
)

