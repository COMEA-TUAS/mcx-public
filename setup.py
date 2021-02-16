import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

print(setuptools.find_packages(where='src'))

setuptools.setup(
    name="modelconductor",
    version="0.0.2",
    author="Panu Aho",
    author_email="panu.aho@gmail.com",
    description="Many To Many Co-Simulation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/donkkis/modelconductor",
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
