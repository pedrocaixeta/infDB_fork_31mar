# Welcome to infDB :simple-rocket:

<p align="center">
  <img src="assets/img/logo_infdb_text.png" alt="infDB logo" width="100"/>
</p>

The **infDB - Infrastructure and Energy Database** provides a modular and easy-to-configure open-source data and tool infrastructure equipped with essential services, designed to minimize the effort required for data management. Its primary mission is to empower the growth of an ecosystem by offering standardized interfaces and APIs. This platform-independent approach streamlines collaboration in energy modeling and analysis, allowing users to dedicate their focus to generating insights rather than handling data logistics.

## Key Features

: :material-plus-circle: **Geospatial, Time Series & Graph Data Support**: Built on PostGIS, TimescaleDB and pgRouting.
: :material-plus-circle: **Platform Independent**: Containerized with Docker.
: :material-plus-circle: **Modular**: extensible via standardized APIs.
: :material-plus-circle: **Open Source**: Apache License Version 2.0 - permissive licensing.
: :material-plus-circle: **Open Data**: Automatized import of common opendata sources.

## Why use it?

The infDB platform addresses common challenges in energy system modeling and analysis, particularly those related to data management. By providing a standardized and modular infrastructure, infDB reduces the time and effort required to set up and maintain data systems. This allows researchers, analysts, and planners to focus on their core tasks of modeling and analysis, rather than being bogged down by data logistics.

The infDB can be used effectively wherever geospatial and time series information is required. Possible applications include:

-   Energy System Modeling
-   Municipal Heat Planning and Infrastructure Planning
-   Scenario and Geospatial Analysis

## Architecture

The infDB architecture is composed of two main components:

: :fontawesome-solid-gears: **[Services](usage/services.md)** – Dockerized open-source software providing base functionality.
: :material-tools: **[Tools](tools/index.md)** – Software interacting with the infDB.

![infDB Overview](assets/img/infdb-overview.png)

## Getting Started

Check out the **[Usage Guide](usage/index.md)** to install, configure and run your instance.

## Demo
The **[Linear Heat Density](linear-heat-density/index.md)** use case demonstrates the capabilities of infDB in the context of municipal heat planning (KWP). 
![](linear-heat-density/qwc-browser.png)

## Contribution

Check out the **[Developer Guide](development/index.md)** to learn how to contribute.

## Feedback and contributions

The content of this documentation is brand new! If you encounter a mistake, notice missing content, or have any other input, please get in touch on [GitHub discussions](https://github.com/infDB/infDB/discussions), or submit an issue.