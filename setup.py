from setuptools import setup, find_packages

setup(
    name="asyncs7_py",
    version="0.1.0",
    packages=find_packages(),# Automatically find all packages (e.g., my_package)
    install_requires=[],
    dependency_links=[],
    author="ZisIsNotZis",
    author_email="ZisIsNotZis@Gmail.com",
    description="A lightweight, async Python library for communicating with Siemens S7 PLCs over Ethernet/IP. This library provides an asynchronous interface for reading data blocks (DBRead) from S7 PLCs, with a focus on simplicity and proper asyncio usage.",
    url="https://github.com/ZisIsNotZis/asyncs7.py",
)
