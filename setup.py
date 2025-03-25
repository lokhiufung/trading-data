from setuptools import setup, find_packages

# Load requirements from the file
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name='trading-data',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'trading-data=trading_data.cli:cli',  # assumes you have a `cli.py` with a `cli()` function
        ],
    },
    description='Data management CLI and Datalake client for trading datasets',
    author='Fisher Lok',
)