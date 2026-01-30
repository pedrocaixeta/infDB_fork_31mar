---
# title: 'infDB: An Open-Source Infrastructure and Energy Database'
title: 'infDB: An Open-Source Data Ecosystem for Urban Energy Infrastructure Modelling and Planning'
tags:
    - Python
    - Docker
    - Open Data
    - 'Data Science'
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
      orcid: '0000-0001-6560-134X'
      equal-contrib: false
      affiliation: 1
    - name: Marvin Wen Huang
      orcid: 0009-0002-3988-2138
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
    - name: Laura Kuper
      orcid: ''
      equal-contrib: false
      affiliation: 2,3
    - name: Carolin Ayasse
      orcid: 0009-0006-8478-216X
      equal-contrib: false
      affiliation: 3
    - name: Haniyeh Ebrahimi Salari
      orcid: ''
      equal-contrib: false
      affiliation: 4
    - name: Martin Stengel
      orcid: 0009-0006-2721-3227
      equal-contrib: false
      affiliation: 1,5
affiliations:
    - name: Technical University of Munich, Germany
      index: 1
      ror: 02kkvpp62
    - name: Siemens AG, Germany
      index: 2
      ror: 
    - name: Technical University of Darmstadt, Germany
      index: 3
      ror: 05n911h24
    - name: Technical University of Dortmund, Germany
      index: 4
      ror: 
    - name: Rosenheim Technical University of Applied Sciences, Germany
      index: 5
      ror: 
date: '31 January 2026'
bibliography: paper.bib
---

# Summary
`infDB - Infrastructure and Energy Database` is an open-source, containerized data infrastructure for managing and providing access to heterogeneous energy and infrastructure datasets used in urban and regional energy system studies. It bundles a PostgreSQL-based database with geospatial and time-series extensions, standardized REST and OGC-compliant APIs, and configurable import services that transform raw public data into structured, version-controlled schemas.

By decoupling data ingestion, storage, and access from downstream analysis and modeling tools, infDB reduces data preprocessing effort and enables reproducible, transferable energy modeling workflows across regions and projects. In that way it can be used for many different applications in energy system modeling, such as district heating planning, electrical distribution network analysis, or urban energy demand estimation.
  

# Statement of need
The transition to a climate-neutral energy system is a central pillar of energy policy, exemplified by Germany's aim for climate neutrality by 2045. New legislative frameworks, such as the requirement for municipal heat planning (KWP) and the requirement for grid expansion plans based on regional transition pathway scenarios defined in the German Energy Industry Act (EnWG §14d), demand that municipalities and Distribution System Operators (DSOs) process vast amounts of energy and infrastructure data [@kwp:2026; @14d:2026].

However, the current landscape of energy data is fragmented. While the Open Data Strategy of the Federal German Government [@Open-Data-Strategie:2021] has increased data availability, this data is published by disparate authorities on different platforms in varying formats, spatial resolutions, and licensing structures. Consequently, energy modeling workflows often suffer from:

1. **High Pre-processing Effort:** Researchers and professionals spend disproportionate time acquiring raw data before analysis.
2. **Limited Workflow Transferability:** Data processing workflows often require substantial adaptation across regions due to differing data formats, interfaces, and conventions.
3. **Lack of Reproducibility:** Planning results are often one-off studies that are difficult to update or audit.
4. **Siloed Infrastructure:** DSOs and municipalities lack standardized tools to efficiently manage and share energy and infrastructure data, such as distribution network or building data.

`infDB` addresses these challenges by providing a reproducible, version-controlled, and automated ETL (Extract, Transform, Load) pipeline. It acts as a middleware between raw public data and high-level energy modeling tools, ensuring that planning data is transparent, traceable, and easily updatable. 
While `infDB` is developed in a German context addressing local regulatory requirements, the underlying architecture and methodology can be also applied to data across different regions and regulatory frameworks, making it applicable to municipal and regional energy planning efforts worldwide.

# State of the field
Energy and infrastructure data management is an active field with several existing solutions:

* **Commercial Platforms:** Tools like **nPro**, **Solarea**, and **flexRM** offer robust analytics and user interfaces but are proprietary, limiting transparency and community extension.
* **Open Source Modeling Frameworks:** Tools like **City Energy Analyst (CEA)**, **EUReCA** and **OpenPlan** focus on modeling and optimization but typically assume preprocessed, structured input data provided externally.
* **Data Initiatives:** The **NEED project** [@NEED:2023] provides a decentralized data hub for synthetic energy data, and **DB4KWP** [@DB4KWP:2026] focuses on ontologies (OEO/OEKG) and naming conventions.

`infDB` fills a gap in the existing energy data ecosystem. While simulation tools like the City Energy Analyst focus on modeling, `infDB` provides the foundational data infrastructure that these tools require. Unlike static data repositories, `infDB` offers a dynamic, service-oriented platform that enables users to deploy local instances, continuously integrate fresh datasets, and seamlessly connect with both commercial and open-source downstream tools. By providing a technical implementation layer, `infDB` can complement ontology initiatives like DB4KWP, transforming conceptual data standards into practical, operational systems.

###Review: "transforming conceptual data standards into practical, operational systems." - really? In NEED I agree but not in infdb? Or maybe you have to be more clear here what you mean?###

# Software Design
`infDB` is designed as a modular, containerized data infrastructure that decouples data ingestion, storage, and access from downstream analysis and modeling. It follows a service-oriented architecture orchestrated via Docker Compose, allowing individual components to be deployed, configured, and combined depending on the requirements of a specific workflow. The system is conceptually divided into **Services**, which provide the foundational infrastructure, and **Tools**, which consume and process the data. This separation allows for high data portability between the tools. The configuaribilty of the services enables users to activate only the components required for their specific use case.

###REVIEW: I wouldn't say that the separation into Services and Tools enables targeted component selection but the configuarbility and maybe the modular structure.###

![infDB - Data Sources, Services and Tools \label{fig:infdb-overview}](docs/mkdocs/docs/assets/img/infdb-overview.png)

<!-- ### infDB - Services -->
The *Services* layer (depicted in the grey box in \autoref{fig:infdb-overview}) handles database operations, administration, data ingestion, and connectivity. These containerized services include:

* **infdb-import:** This service automates the ingestion of heterogeneous external data sources. It transforms raw external formats into structured schemas within the database. Users control this process via a simple YAML configuration file (`config-infdb-import.yml`), eliminating the need for custom ETL scripting.
* **infdb-db:** The central storage engine hosting a PostgreSQL database. It is pre-configured with essential extensions for energy modeling:
    * **PostGIS** for geospatial data.
    * **TimescaleDB** for time-series data.
    * **3D City DB** for (3D) semantic city models.
    * **pgRouting** for graph-based network analysis.
* **APIs and Data Access Services:** infDB exposes data exclusively through standardized interfaces rather than custom file formats. This includes SQL access to the database as well as RESTful and OGC-compliant APIs implemented using **FastAPI**, **PostgREST**, and **pygeoAPI**. These interfaces allow external tools to access data in a consistent manner while keeping the internal database structure encapsulated.
* **Administrative and Interactive Services:** Optional services such as **pgAdmin**, **Jupyter Notebook**, and a web-based **QGIS** client support administration, inspection, prototyping, and visualization of data stored in the database. These components are intended to lower the entry barrier for users from different backgrounds (e.g., GIS specialists or researchers). However, they are not required for automated or headless workflows.
* **External Storage Integration:** An optional **OpenCloud** component allows integration with cloud-based storage solutions for handling large datasets. This component is not required for local deployments and can be omitted in minimal setups.

<!-- ### infDB - Tools -->
The *Tools* layer (depicted in the right box in the architecture diagram) consists of (external) software that interacts with the `infDB` Services to process data or generate insights. Each tool can interact independently with the Services, reading and writing data. This modular approach allows users to chain different tools into custom workflows. Depending on the tool type and requirements, e.g. new scripts, open tool, proprietary tool, multiple integration options with the `infDB` are available building upon following foundations:

###REVIEW: I think the description around the tools might be a bit confusing for people who are new to infdb. Especially, the distinction between different types of tools could be more precise. A question that might arise for someone with a tool is: 'How do I couple my tool? Can I just use infdb for data, or do I have to integrate my tool into infdb?' I think this is partially a wording issue; for instance, you speak of (external) tools but then only of 'building tools'. Additionally, I think it would be helpful to highlight early on that different types of tools can be integrated in different ways. ###

###REVIEW: To me, it is not clear which type of modularity is meant here: Is it the tools being distinct from one another, or the separation into Services and Tool? I made a suggestion for an extra sentence, but I am not sure if my interpretation regarding modularity is correct.###


* **Standardized Integration:** Tools interact with the core database exclusively through open interfaces (SQL or REST APIs), ensuring that the underlying data schema remains consistent regardless of the tool used.
* **pyinfdb:** To facilitate the development of custom tools, the platform provides the `pyinfdb` Python package. This library abstracts database connections, logging, and configuration management, allowing researchers to rapidly develop Python-based analysis scripts that integrate seamlessly with the infDB ecosystem.
* **Extensible Ecosystem:** Importantly, infDB does not prescribe specific modeling approaches, optimization methods, or planning workflows. Its role is limited to providing a stable and reproducible data infrastructure that can be reused across different analytical contexts. Therefore, users can integrate existing third-party simulation software or develop proprietary tools that plug into the `infDB` backend without modifying the core services.

# Research Relevance
The research relevance of `infDB` lies in its role as a reusable data infrastructure that supports transparent and reproducible energy system analysis workflows. By separating data management from analysis logic, `infDB` contributes to several recurring methodological requirements in energy research:

* **Reproducibility:** Containerized deployment and configuration-based data ingestion allow complete data pipelines to be rerun and inspected. This enables studies to be reproduced or updated when new data becomes available, without reimplementing preprocessing steps.
* **Transferability:** By enforcing structured schemas and standardized access interfaces, infDB allows data processing and analysis workflows to be reused across regions and projects with minimal adaptation, reducing region-specific reimplementation effort.
* **Separation of Concerns:** infDB decouples data acquisition, storage, and access from modeling and analysis code. This allows researchers to develop and test analytical methods independently of changes in input data formats or sources.
* **Workflow Stability:** Stable database schemas and interfaces provide a consistent foundation for iterative research, supporting comparative studies and sensitivity analyses without requiring repeated adjustments to data handling logic.
* **Methodological Neutrality:** infDB does not prescribe modeling approaches, optimization methods, or policy assumptions. Its role is limited to providing structured and accessible data, allowing a wide range of analytical methods to be applied without constraint.

# Applications
One exemplary use case is calculating the linear heat density by estimating building heat demands and distributing them along street segments, to assess the financial feasibility of district heating as basis for municipal heat planning (KWP) or district heating feasibility studies (BEW):

**Step 1 – Data Integration (Service):** Raw, heterogeneous data sources (building registries, census data, street network geometries, energy consumption records) are ingested through `infdb-import` and stored in the unified database, eliminating manual data collection and format conversion.

**Step 2 – Data Access & Enrichment (Service):** Analysis tools query the standardized database via REST or SQL APIs to retrieve preprocessed, enriched geospatial datasets (e.g., LOD2 building models with thermal properties). This replaces tedious file-based GIS workflows.

**Step 3 – Heat Demand Estimation (Tool):** Researchers apply domain-specific algorithms to compute building-level heat demands using statistical models calibrated against census and consumption data, leveraging the consistent data foundation that `infDB` provides.

**Step 4 – Linear Heat Density Calculation (Tool):** Building heat demands are spatially distributed along street network segments to derive linear heat density maps. This metric directly informs feasibility assessments by identifying corridors where district heating networks are economically viable.

**Step 5 – Iteration & Update:** When new data becomes available (e.g., updated building stock, revised energy statistics), the entire pipeline can be re-executed without reimplementing preprocessing logic, ensuring studies remain current and reproducible.

Another use case is generating synthetic data of the existing low-voltage grid structures to analyse complex impacts of new assets and their optimized management on grid reinforcement requirements.
By building upon `infDB`, researchers and planners can immediately leverage preprocessed, enriched open data such as building data (LOD2) and statistical census information to generate consistent synthetic base datasets. 
The availability of this base data for different use cases facilitate the seamless integration of downstream tools such as energy system optimization models with consistent data for both electrical and district heating grids.
Compared to isolated file-based GIS workflows this interoperable approach reduces computational overhead as well as development and streamlining efforts, while providing reproducible, auditable results that can be continuously updated as new data becomes available.

# AI usage disclosure
In the development of this work, GitHub Copilot, ChatGPT, and Gemini were used. Github Copilot assisted in code generation by code suggestions and completion tasks, while ChatGPT and Gemini assisted in drafting and refining textual content.

# Acknowledgements
Martin Stengel gratefully acknowledges financial support through the Bavarian State Ministry of Science and the Arts to promote applied research and development at universities of applied sciences and technical universities.
All other authors gratefully acknowledge financial support through the project executing agency Jülich (PTJ) with funds provided by the Federal Ministry for Economic Affairs and Climate Action (BMWK) due to an enactment of the German Bundestag under Grant No. 01256602/1.

# References

