from setuptools import find_packages, setup

setup(
    name="datadazed_dagster",
    version="0.0.1",
    author_email="trevor.barnes91@gmail.com",
    packages=find_packages(exclude=["test"]),
    include_package_data=True,
    install_requires=[
        "dagster",
        "dagit",
        "dagster-dbt",
        "dagster-airbyte",
        "dagster-postgres",
        "dbt-core",
        "pandas",
    ],
    author="Trevor",
    license="Apache-2.0",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
