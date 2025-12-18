# Welcome to infDB - Infrastructure and Energy Database

**The infDB (Infrastructure and Energy Database) is a user-friendly, platform-independent, and open-source data infrastructure as a foundation for energy system analyses. It enables complex evaluations by combining various tools through standardized interfaces, fostering an open and interoperable ecosystem.**


<!-- ## Table of Contents

- [Purpose](#purpose)
- [How it works?](#how-it-works)
- [Getting Started](#getting-started)
  - [Installation for local development](#installation-for-local-development)
- [For Developers](#for-developers)
   - [Repository Structure](#repository-structure)
   - [Usage Guidelines](#usage-guidelines)
   - [Basic API Usage](#basic-api-usage)
   - [Development Workflow](#development-workflow)
   - [API Documentation](#api-documentation)
   - [CI/CD Workflow](#cicd-workflow)
   - [Development Resources](#development-resources)
   - [Contribution and Code Quality](#contribution-and-code-quality)
- [License and Citation](#license-and-citation) -->

## Purpose

The infDB offers a flexible and easy-to-configure data infrastructure with essential services, minimizing the effort required for data management. By providing standardized interfaces and APIs, infDB streamlines collaboration in energy modeling and analysis, enabling users to focus on insights rather than data handling.


## Applications
The infDB is useful if you need to deal with geometric and time series data at the same time as for instance:
- Energy System Modeling
- Infrastructure Planning
- Scenario Analysis
- Geospatial Analysis

**Linear Heat Density**

## How it works?
The infDB architecture is composed of three coordinated layers as shown in the figure below:

1. **Core** – foundational geospatial and semantic infrastructure and energy database
2. **Services** – preconfigured platform services (left & top)
3. **Tools** – external connected software and scripts. (right)

The PostgreSQL database is the basis and extended by services and tools. More information of each layer is described below. 
The PostgreSQL, all services and adopted tools are dockerized for a modular and flexible application.
![alt text](img/infdb-overview.png)

More information in detail can be found on section infDB.

## License and Citation

The code of this repository is licensed under the **MIT License** (MIT).  
See [LICENSE](LICENSE) for rights and obligations.  
See the *Cite this repository* function or [CITATION.cff](CITATION.cff) for citation of this repository.  
Copyright: [TU Munich - ENS](https://www.epe.ed.tum.de/en/ens/homepage/) | [MIT](LICENSE)

## Contact
Patrick Buchenberg

Chair of Renewable and Sustainable Energy System - Technical University of Munich (TUM).
Email: patrick.buchenberg@tum.de
[https://www.epe.ed.tum.de/ens/staff/ensteam/patrick-buchenberg/](https://www.epe.ed.tum.de/ens/staff/ensteam/patrick-buchenberg/)
