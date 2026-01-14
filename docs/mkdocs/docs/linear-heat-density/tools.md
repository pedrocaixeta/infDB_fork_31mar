# Tools

This section provides an overview of the used open source tools for the linear heat density calculation and processing.

## infdb-basedata
For the generation of the base data used within the linear heat density toolchain, the `infdb-basedata` tool is applied. More information about the tool and its functionalities can be found at Tools -> **[infdb-basedata](../tools/infdb-basedata.md)**.

## ro-heat
The `ro-heat` tool is used to estimate the building heat demand on a building level. It uses statistical data and building characteristics to calculate the heat demand for each building in the dataset.

## process-streets
The `process-streets` tool is responsible for identifying suitable streets for district heating based on various criteria such as building density, street length, and connectivity. It processes the street network and filters out streets that do not meet the requirements for district heating.

## linear heat density
The `linear-heat-density` tool calculates the linear heat density by aggregating the heat demand of buildings along each street segment and dividing it by the length of the street. This results in a metric that indicates the amount of heat demand per unit length of a street, which is crucial for assessing the feasibility of district heating systems.