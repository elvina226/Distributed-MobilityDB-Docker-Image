**Distributed MobilityDB Cluster for Spatiotemporal Maritime Data**

**What the project does**

This project provides a reproducible Docker-based environment for running PostgreSQL 16.9 with MobilityDB, Citus, and a patched version of DistributedMobilityDB. The environment was developed for the Master's thesis "A benchmark for maritime data processing and analytics" on systems analyzing spatiotemporal data, with a specific focus on maritime analytics using the AIS Piraeus dataset.

It includes:

A Citus Docker file (PostgreSQL 16.9 + MobilityDB + Citus).

A Citus Docker Image Documentation

A DistributedMobilityDB Docker file (PostgreSQL 16.9 + MobilityDB + Citus + patched DistributedMobilityDB).

A DistributedMobilityDB Docker Image Documentation

A DistributedMobilityDB Cluster Setup Documentation (to deploy coordinator and worker nodes, configure networking, authentication, and required extensions).

A Code folder (SQL, Python, R)

**Why the project is useful**

Spatiotemporal data analytics, especially for maritime datasets like AIS, is computationally demanding. This setup demonstrates how to:

Scale queries with Citus (distributed PostgreSQL).

Use MobilityDB for temporal and spatial queries on moving objects.

Experiment with DistributedMobilityDB for distributed spatiotemporal workloads.

Use different types of queries to test the perfomance of the systems.

It serves as a reference for researchers or practitioners interested in large-scale trajectory data analysis.

**How users can get started**

Build or pull the Docker images included in this repository.

Follow the cluster setup guide to start a coordinator and worker containers using the provided network configuration.

Enable the required extensions (citus, postgis, mobilitydb, distributed_mobilitydb) on all nodes.

Load your dataset (e.g. AIS Piraeus dataset) and begin experimenting with distributed spatiotemporal queries.

Use the Python script for cleaning and preprocessing of the AIS dataset before loading it into the database.

Use the SQL files for database setup, schema definitions, and the benchmark queries.

Use the R script for creating Figures summarizing runtime performance for the benchmark queries.

**For detailed steps, see the provided documentation:**

Citus Docker Image.docx  

DistributedMobilityDB Docker Image.docx  

DistributedMobilityDB Cluster Setup.docx 

**Where users can get more information**

Distributed MobilityDB documentation:https://github.com/mbakli/DistributedMobilityDB

MobilityDB documentation: https://mobilitydb.com, https://github.com/MobilityDB/MobilityDB

Citus documentation: https://www.citusdata.com, https://github.com/citusdata/citus

PostgreSQL documentation: https://www.postgresql.org/docs

