from pandas import DataFrame, Series

from . import eureca_code


def calculate_rc_values(elements: DataFrame) -> DataFrame:
    elements["materials"] = create_materials(elements)
    constructions = create_constructions(elements)
    return aggregate_rc_values(constructions)


def create_materials(elements: DataFrame) -> Series:
    return elements.apply(
        lambda x: eureca_code.Material(
            x["name"],
            x["thickness"],
            x["thermal_conduc"],
            x["heat_capac"],
            x["density"],
        ),
        axis=1,
    )


def create_constructions(elements: DataFrame) -> DataFrame:
    # Group materials to a list by building_objectid and element_name, drop multi-index
    constructions = (
        elements.groupby(["building_objectid", "element_name", "area"])["materials"]
        .apply(list)
        .reset_index()
    )

    # Map tabula to EUReCA names
    tabula_eureca_element_name_mapping = {
        "GroundFloor": "GroundFloor",
        "OuterWall": "ExtWall",
        "Rooftop": "Roof",
        "Ceiling": "IntCeiling",
        "Floor": "IntFloor",
        "InnerWall": "IntWall",
        "Window": "Window",
    }

    # Create a EUReCA Constructions per building_objectid from the list of materials and assign the correct EUReCA type
    constructions["construction_obj"] = constructions.apply(
        lambda row: eureca_code.Construction(
            name=f"B{row['building_objectid']}_{row['element_name']}",
            materials_list=row["materials"],
            construction_type=tabula_eureca_element_name_mapping[row["element_name"]],
        ),
        axis=1,
    )

    return constructions


def aggregate_rc_values(constructions: DataFrame) -> DataFrame:
    # Internal elements such as inner walls, floors or ceilings do not contribute to thermal resistance
    constructions["resistance"] = constructions.apply(
        lambda row: row["area"] / row["construction_obj"].thermal_resistance if row['element_name'] in ["OuterWall",
                                                                                                        "GroundFloor",
                                                                                                        "Rooftop",
                                                                                                        "Window"] else 0,
        axis=1,
    )

    constructions["capacitance"] = constructions.apply(
        lambda row: row["construction_obj"].k_int * row["area"], axis=1
    )

    rc_values = (
        constructions.groupby("building_objectid")[["capacitance", "resistance"]]
        .sum()
        .sort_values("building_objectid")
    )
    rc_values["resistance"] = 1 / rc_values["resistance"]
    return rc_values
