from setuptools import find_packages, setup

setup(
    name="datastack_orchestration",
    packages=find_packages(exclude=["datastack_orchestration_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
