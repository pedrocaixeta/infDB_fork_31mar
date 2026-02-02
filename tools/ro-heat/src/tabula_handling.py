import numpy as np

from . import eureca_code


def create_tabula_structure(tabula_rows):
    tabula_rows['materials'] = tabula_rows.apply(
        lambda x: eureca_code.Material(
            x["material_name"],
            x["thickness"],
            x["thermal_conduc"],
            x["heat_capac"],
            x["density"],
        ),
        axis=1,
    )

    # TODO: Explicitly sort by layer_index according to EUReCA specification
    constructions = (
        tabula_rows.groupby(["building_type", "element_name", "construction_data", "start_year", "end_year"])[
            "materials"]
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
    construction_objects = constructions.apply(
        lambda row: eureca_code.Construction(
            name=f"B{row['construction_data']}_{row['element_name']}_{row['start_year']}_{row['end_year']}",
            materials_list=row["materials"],
            construction_type=tabula_eureca_element_name_mapping[row["element_name"]],
        ),
        axis=1,
    )
    constructions["R"] = construction_objects.apply(lambda x: x.thermal_resistance)
    constructions["C"] = construction_objects.apply(lambda x: x.k_int)
    constructions["C"] = constructions["C"].fillna(0.0)

    return constructions


def calculate_rc_values(tabula, row):
    overall_r = 0.0
    overall_c = 0.0
    components = tabula['element_name'].unique()
    for component in components:
        component_tabula = tabula[tabula.element_name == component]
        match = resolve_construction(component_tabula, component, row)

        if component in ['Ceiling', 'Floor']:
            area = row['floor_area'] * max(row['floor_number'] - 1, 1)
        elif component == 'InnerWall':
            area = row['floor_area'] * max(row['floor_number'] - 1, 1) * 2.5
        elif component == 'Rooftop':
            area = row['roof_area']
        elif component == 'OuterWall':
            area = row['wall_area']
        elif component == 'Window':
            area = row['window_area']
        elif component == 'GroundFloor':
            area = row['floor_area']
        else:
            raise Exception(f"Unknown component: {component}")

        # Only "OuterWall","GroundFloor", "Rooftop","Window" contribute to R value
        if component in ["OuterWall", "GroundFloor", "Rooftop", "Window"]:
            overall_r = overall_r + (area / match['R'])
        overall_c = overall_c + (match['C'] * area)

    return 1 / overall_r, overall_c


def resolve_construction(tabula, component, row):
    # TODO: Document and simplify
    if component in ['GroundFloor', 'Ceiling', 'Floor', 'InnerWall']:
        year = row['construction_year']
    elif component == 'Rooftop':
        year = row['rooftop']
    elif component == 'OuterWall':
        year = row['outer_wall']
    elif component == 'Window':
        year = row['window']
    else:
        raise ValueError(f"Unknown construction type: {component}")

    if component in ['Ceiling', 'Floor', 'InnerWall']:
        building_type = 'standard'
        construction_string = 'tabula_de_standard'
    else:
        building_type = row['building_type']
        if year == row['construction_year']:
            construction_string = f'tabula_de_standard_1_{building_type}'
        else:
            construction_string = f'tabula_de_retrofit_1_{building_type}'

    candidates = tabula[(tabula.building_type == building_type)
                        & (tabula.construction_data == construction_string)]

    if candidates.empty:
        raise ValueError()

    # Check for direct match
    match = candidates[
        (candidates.start_year <= year)
        & (candidates.end_year >= year)]

    # If there's no direct match, fall back to closest
    if match.empty:
        dist = np.where(
            year < candidates.start_year.to_numpy(),
            candidates.start_year.to_numpy() - year,
            year - candidates.end_year.to_numpy()
        )

        # argmin = index of closest range
        i = np.lexsort((candidates.start_year.to_numpy(), dist))[0]
        match = candidates.iloc[i]
    else:
        # Convert DataFrame to Series for
        match = match.iloc[0]

    return match
