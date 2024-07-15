from setuptools import setup, find_packages

setup(
    name="pydoopfsspec",
    version="0.1.0",
    description="A pydoop fsspec implementation for Hadoop filesystem",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/pwang697/PydoopFSSpec",
    packages=find_packages(include=["pydoopfsspec", "pydoopfsspec.*"]),
    include_package_data=True,
    install_requires=[
        "fsspec",
        "pydoop",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    entry_points={
        "fsspec.specs": [
            "pydoop=pydoopfsspec.HadoopFileSystem"
        ]
    }
)
