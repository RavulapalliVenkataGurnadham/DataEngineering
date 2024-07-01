from setuptools import find_packages, setup

setup(
    name='pyspark_data_profiling',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark=3.2.4',
        'pyyaml>=5.3.1'
    ],
    entry_points={
        'console_scripts': [
            'run-numeric-profiling=profiling.numeric_profiling:run_numeric_profiling',
            'run-categorical-profiling=profiling.categorical_profiling:run_categorical_profiling',
        ],
    },
)
