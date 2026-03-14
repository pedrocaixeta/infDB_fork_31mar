<p align="center">
   <img src="docs/mkdocs/docs/assets/img/logo_infdb_text.png" alt="Repo logo" width="100"/>
</p>

# infDB - Infrastructure and Energy Database
**infDB - Infrastructure and Energy Database** provides a modular and easy-to-configure open-source data and tool infrastructure equipped with essential services, designed to minimize the effort required for data management. This platform-independent containerized approach streamlines collaboration in energy modeling and analysis, empowering the growth of an ecosystem by offering standardized interfaces and APIs, and allowing users to dedicate their focus to generating insights rather than handling data logistics by ensuring data is FAIR (Findable, Accessible, Interoperable, and Reusable).

| Category | Badges |
|----------|--------|
| License | [![License](https://img.shields.io/badge/license-Apache%202-blue)](LICENSE) |
| Documentation | [![Documentation](https://img.shields.io/badge/docs-available-brightgreen)](https://tum-ens.github.io/InfDB) |
| Community | [![Contributing](https://img.shields.io/badge/contributions-welcome-brightgreen)](https://tum-ens.github.io/InfDB/development) [![Contributors](https://img.shields.io/badge/contributors-0-orange)](#) [![Repo Count](https://img.shields.io/badge/repo-count-brightgreen)](#) |

## Table of Contents

- [Why use it?](#why-use-it)
- [How it works?](#how-it-works)
- [Getting Started](#getting-started)
- [License and Citation](#license-and-citation)


## Why use it?

The infDB platform addresses common challenges in energy system modeling and analysis, particularly those related to data management. By providing a standardized and modular infrastructure, infDB reduces the time and effort required to set up and maintain data systems. This allows researchers, analysts, and planners to focus on their core tasks of modeling and analysis, rather than being bogged down by data logistics.

The infDB can be used effectively wherever geospatial and time series information is required. Possible applications include:

-   Energy System Modeling
-   Municipal Heat Planning and Infrastructure Planning
-   Scenario and Geospatial Analysis

<!-- ## Purpose
**infDB (Infrastructure and Energy Database)** offers a flexible and easy-to-configure data infrastructure with essential services, minimizing the effort required for data management. By providing standardized interfaces and APIs, infDB streamlines collaboration in energy modeling and analysis, enabling users to focus on insights rather than data handling.

For instance, it can be used for the following applications:
- Energy System Modeling
- Infrastructure Planning
- Scenario Analysis
- Geospatial Analysis -->

## How it works?
The infDB architecture is composed of three coordinated layers as shown in the figure below:

- **Services** – Dockerized open-source software providing base functionality.
- **Tools** –  Software interacting with the infDB.

The PostgreSQL database is the basis and extended by services and tools. More information of each layer is described below. 
The PostgreSQL, all services and adopted tools are dockerized for a modular and flexible application.
![infDB overview](docs/mkdocs/docs/assets/img/infdb-overview.png)

### Services
The infDB platform provides a suite of essential services designed to facilitate database operation and administration, data handling and visualization, and connectivity. Each preconfigured service can be activated individually to tailor the environment to your specific requirements. This section provides a brief description and configuration options for each available service.

More information, a list of available services see [Services](https://tum-ens.github.io/InfDB/infdb/#services).

### Tools
Tools are software that interact with infDB and process data through standardized, open interfaces. This modular approach allows you to tackle problems of any complexity by combining different tools into custom toolchains.

More information, a list of integrated tools and additional information, see [Tools](https://tum-ens.github.io/InfDB/tools/).

## Getting Started
If you want to use the infDB with the default settings just use the [Quick Start](#Quick-Start) below. For more information in detail read the [Usage Guide](https://tum-ens.github.io/InfDB/usage/) of the official documentation.

### Prequisites
 - Docker Engine: https://docs.docker.com/engine/install/
 - Docker Desktop: https://docs.docker.com/desktop/

### Folder Structure
The infDB uses the following folder structure:
  ```
  infdb/
  ├── infdb-demo/
  ├── sonthofen/
  ├── ...
  └── muenchen/
```

### Quick Start
You can quickly start an infDB with default configuration and credentials by following these steps:

#### Create Folder for infDB
```bash
mkdir infdb
cd infdb
```

#### Clone infDB
``` bash
# Replace "infdb-demo" by name of instance 
git clone git@github.com:tum-ens/InfDB.git infdb-demo
cd infdb-demo
```

#### Start infDB
```bash
bash infdb.sh start
```

#### Import Opendata
```bash
bash infdb.sh import
```

#### Stop infDB
```bash
bash infdb.sh stop
```

<!-- # Changelog

The changelog is maintained in the [CHANGELOG.md](CHANGELOG.md) file. It lists all changes made to the repository. Follow instructions there to document any updates. -->

# License and Citation

The code of this repository is licensed under the **Apache 2.0 License**.  
See [LICENSE](LICENSE) for rights and obligations. See [Citation](docs/mkdocs/docs/welcome/citation.md) for citation of this repository.  
Copyright: [TU Munich - ENS](https://www.epe.ed.tum.de/en/ens/homepage/) | [Apache 2.0 License](LICENSE)

# Contact
Patrick Buchenberg

Chair of Renewable and Sustainable Energy System
Technical University of Munich (TUM) 
Email: patrick.buchenberg@tum.de
[https://www.epe.ed.tum.de/ens/staff/ensteam/patrick-buchenberg/](https://www.epe.ed.tum.de/ens/staff/ensteam/patrick-buchenberg/)