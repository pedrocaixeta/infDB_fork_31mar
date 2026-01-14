# Linear Heat Density Use Case

The underlying linear heat density toolchain estimates the linear heat density of streets in a given area. The linear heat density is a key metric for assessing the feasibility and efficiency of district heating systems. It is defined as the amount of heat demand per unit length of a street.

The toolchain exists of several steps:

1. The building heat demand is estimated on a building level using statistical data and building characteristics. 
2. Suitable streets for district heating are identified based on various criteria such as building density, street length, and connectivity. 
3. The linear heat density is calculated by aggregating the heat demand of buildings along each street segment and dividing it by the length of the street.

![alt text](linear-heat-density.jpg)

The whole linear heat density process is implemented through a combination of open-source tools and custom scripts tailored to specific requirements, executed within the infDB environment.

## Run Linear Heat Density
To run the complete toolchain of linear heat density, use the following command:
```bash
bash tools/run_linear-heat-density.sh
```
The infDB connects then several tool in order to determine the linear heat density by estimating the heat demand on a building level and procesing suited streets for district heating.

## Toolchain
The toolchain of the Linear Heat Density is composed of several tools. Each tool creates a new schema storing the results.
![Toolchain](toolchain.png)


