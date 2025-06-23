from setuptools import setup, find_packages

setup(
    name="lakehouse-iceberg-dataflow",
    version="1.0.0",
    description="GCP Lakehouse Iceberg Dataflow Templates",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "apache-beam[gcp]==2.53.0",
        "google-cloud-bigquery>=3.11.0",
        "google-cloud-storage>=2.10.0",
    ],
) 