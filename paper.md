---
# title: 'infDB: An Open-Source Infrastructure and Energy Database'
title: 'infDB: An Open-Source Data Ecosystem for Urban Energy Infrastructure Modelling and Planning'
tags:
    - Python
    - Docker
    - Open Data
    - Database
    - Data Science
authors:
    - name: Patrick Buchenberg
      orcid: 0000-0001-7683-8422
      corresponding: true
      equal-contrib: false
      affiliation: 1
    - name: Markus Döpfert
      orcid: ''
      equal-contrib: false
      affiliation: 1
    - name: Beneharo Reveron Baecker
      orcid: ''
      equal-contrib: false
      affiliation: 1
    - name: Marvin Wen Huang
      orcid: ''
      equal-contrib: false
      affiliation: 1
    - name: Kadir Kalkan
      orcid: ''
      equal-contrib: false
      affiliation: 1
    - name: Hussein Mohamed Ali Genena
      orcid: ''
      equal-contrib: false
      affiliation: 1
affiliations:
    - name: Technical University of Munich, Germany
      index: 1
      ror: 02kkvpp62
date: '31 January 2026'
bibliography: paper.bib
---

# Summary
The infDB - Infrastructure and Energy Database provides a modular and easy-to-configure open-source data infrastructure for energy modeling and analysis to minimize the effort required for data collection and handling.
The objective is to empower the growth of an ecosystem by offering standardized data formats and interfaces. This platform-independent approach streamlines collaboration in energy modeling and analysis, allowing users to dedicate their focus to generating insights rather than handling data logistics. 

# Statement of need
Municipal Heat Planning (KWP) is an important component for the heating transition and plays a key role in Germany's aim to become climate-neutral by 2045.
The federal government introduced a law in 2023 giving cities with over 100,000 inhabitants until mid-2026 and smaller towns until mid-2028 to come up with a plan for transitioning to climate-neutral heating, which to a large extent is supposed to happen through an expansion of district heating [@Open-Data-Strategie:2021]. However, the majority of these municipal heating plans commissioned in the past are not transparently traceable neither capable of being regularly updated.
Moreover, the paragraph §14d of the German Energy Industry Act (EnWG) obliges operators to provide data on their distribution networks to the Federal Network Agency [@Open-Data-Strategie:2021]. This a huge challenge for DSO (Distribution System Operators) as they often lack standardized data infrastructures and tools to manage and share their data efficiently.
At the same time, the Federal Cabinet pushes the availabilty of public infrastructure and energy data within the scope of the Data Strategy of the Federal German Government [@Open-Data-Strategie:2021]. Most of the required data is now published as open data under licenses like CC-BY 2.0 Germany. The problem is that the data is published by different authorities in different formats and structures. This makes it difficult to access, integrate, and analyze data for energy modeling and policy analysis in practice.

The infDB - Infrastructure and Energy Database as part of the research project NEED addresses these challenges by simplifying and standardizing data access as well as providing open source tools to create a fundamental and reproducible data basis as for both introductory examples mentioned above. It does so by offering a modular and flexible data platform built on containerized services that can be easily activated and configured for specific use cases. This architecture ensures portability across all platforms. By providing standardized interfaces and APIs, infDB fosters an extensible open ecosystem that empowers users to integrate custom tools and workflows seamlessly.

Municipal Heat Planning requires integrating public data from diverse sources and formats as well as transparent and reproducible workflows to ensure that plans can be updated regularly.
The German Energy Industry Act (EnWG) requires DSOs to submit accurate and comprehensive data about distribution networks.
The infDB addresses exactly this need and can be applied in many other contexts where energy and infrastructure data management is required by providing a standardized data infrastructure that supports multiple data formats and sources as well as meets the regulatory requirements efficiently to submit distribution data in compliance with federal standards while reducing administrative burden.

# State of the field
Several commercial and open-source solutions exist for energy and infrastructure data management. Commercial platforms such as nPro, Solarea, and flexRM provide advanced features including user-friendly interfaces and robust analytics tailored to energy professionals. Open-source tools, including OpenPlan and City Energy Analyst, offer frameworks for urban energy system modeling and building energy performance simulation, respectively.

Complementary research initiatives are advancing data standardization efforts. The NEED project develops a decentralized data hub providing conventional and synthetic energy infrastructure data in machine-readable formats to facilitate stakeholder interoperability [@Open-Data-Strategie:2021]. The DB4KWP project addresses data uniformity by formalizing heating data collection, naming conventions, and linking through ontologies such as the Open Energy Ontology (OEO) and Open Energy Knowledge Graph (OEKG) [@Open-Data-Strategie:2021].

Despite these advances, a gap remains: existing solutions lack an integrated, modular data infrastructure platform that seamlessly combines heterogeneous data sources with standardized interfaces. The infDB addresses this gap by providing a flexible, containerized architecture that enables straightforward integration of diverse data sources and emerging solutions, thereby enhancing data quality and ecosystem extensibility.

# Software Design
The infDB is designed to be modular, scalable, and flexible, allowing for easy integration of various data sources and tools. This architecture is implemented using docker-compose to orchestrate multiple services, including the core database, data importers, and various processing tools. The idea is that for each specific use case, only the required services need to be activated and configured accordingly. This architecture ensures portability across all platforms. By providing standardized interfaces and APIs, infDB fosters an extensible ecosystem that empowers users to integrate custom tools and workflows seamlessly.

### Key Features

- **User-Friendly**: easy to configure and use.
- **Geospatial, Time Series & Graph Data Support**: PostgreSQL extended by PostGIS, TimescaleDB and pgRouting.
- **Modular and Platform Independent**: Containerized with Docker.
- **Open Source**: Apache License Version 2.0 - permissive licensing.
- **Open Data**: Automatized import of common opendata sources.

The figure bewlow provides an overview of the infDB architecture and its main components in the grey box. Integrated tools on the right side can access the data stored in the core database via standardized interfaces and data schemas. 

![infDB - Services and Tools](docs/mkdocs/docs/assets/img/infdb-overview.png)

The infDB platform provides a suite of essential *services* designed to facilitate database operation and administration, data handling and visualization, and connectivity. Each preconfigured service can be activated individually to tailor the environment to your specific requirements. 
The *infdb-importer* on the left side inside the grey box facilitates the ingestion of various open data sources into the core database. This service automates the ingestion and structuring of various external open data formats into the infDB platform. It supports multiple data sources and formats, transforming raw data into structured schemas within the database for easy access and analysis. 
The *infdb-db* in the middle of the grey box hosts the PostgreSQL database with extensions for geospatial, time series and graph data (PostGIS, TimescaleDB, pgRouting), serving as the central database within the platform. It handles data storage, retrieval, and management, ensuring integrity and high availability for connected services and tools. 
The *APIs* (fastAPI, pygeoAPI, PostgREST) on the right upper corner inside the grey box provide standardized REST and OGC interfaces for providing data access to external applications and services.

The infDB ecosystem also includes a variety of (external) *tools* designed to handle different aspects of data workflows. These so called tools are software that interact with infDB and process data through standardized, open interfaces. This modular approach allows you to tackle problems of any complexity by combining different tools into custom toolchains and and thus establishes the foundation for an open and extensible ecosystem. In addition to the pre-integrated tools, users can easily integrate their own custom tools into the infDB ecosystem by using the provided python package pyinfdb that can be used to interact with the infDB database and services. It provides functionalities for database connections, logging, configuration management, and utility functions. 


The figure below illustrates the architecture of infDB and its containerized services.

![infDB - Architecture](docs/mkdocs/docs/infdb/infdb-architecture.png)

A central docker compose file (compose.yml) controls all containerized services. Bash scripts (infdb-start.sh, infdb-stop.sh, infdb-remove.sh, infdb-import.sh) are provided to simplify common tasks such as starting, stopping, and managing the entire infDB platform for the end user.
The configuration of the infDB uses environment variables defined in the .env file to customize settings such as database credentials, ports, and controll of services. The imported opendata sources can be managed via a YAML file (config-infdb-loader.yml) file, which specifies datasets to be ingested and their respective configurations.

In summary, the infDB is a modular and flexible data platform built on dockerized services that can be easily activated and configured for specific use cases. This architecture ensures portability across all platforms. By providing standardized interfaces and APIs, infDB fosters an extensible ecosystem that empowers users to integrate custom tools and workflows seamlessly.

# Research Impact Statement
The major research impact of infDB is minimizing the effort required for data management and empowering growth of an ecosystem at the same time. Its ability to streamline and standardize data management for energy system modeling and analysis. By providing a modular, open-source infrastructure, infDB addresses key challenges in data accessibility, integration, and reproducibility. This allows researchers, analysts, and planners to focus on their core tasks of modeling and analysis, rather than being bogged down by data logistics.

The example of calculating linear heat density for municipal heat planning demonstrates how infDB can facilitate complex data workflows by integrating diverse data sources and providing standardized interfaces for data access. This not only speeds up the analysis process but also enhances the quality and reliability of the results.

The long-term impact of infDB is expected to be significant, as it promotes transparency, reproducibility, and collaboration in the energy modeling community. By enabling easy sharing of data and tools, infDB fosters an ecosystem where researchers can build upon each other's work, leading to more open, robust and innovative solutions for energy system challenges. 

The main benefits can be summarized as follows:

- reduce cost and effort of data and infrastructure handling
- speedup research and analysis
- increase quality of research outcomes
- enable and support collaboration
- reproducibility of research results
- transparency


# AI usage disclosure
In the development of this work, GitHub Copilot, ChatGPT, and Gemini were used. Github Copilot assisted in code generation by code suggestions and completion tasks, while ChatGPT and Gemini assisted in drafting and refining textual content.

# Acknowledgements
We gratefully acknowledge financial support through the project executing agency Jülich (PTJ) with funds provided by the Federal Ministry for Economic Affairs and Climate Action (BMWK) due to an enactment of the German Bundestag under Grant No. 03EN3077A.

# References

