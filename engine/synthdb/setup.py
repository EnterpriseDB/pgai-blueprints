# For demonstration purposes only.

from setuptools import setup, find_packages

setup(
    name="edb-synthdb",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "sdv>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "oracledb>=1.0.0",
    ],
    entry_points={
        'console_scripts': [
            'edb-synthdb=edb_synthdb:main',
        ],
    },
)
