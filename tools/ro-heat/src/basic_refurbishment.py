import logging
from typing import Dict, Any

import numpy as np
import pandas as pd
from numpy.random import Generator
from pandas import DataFrame, Series

log = logging.getLogger(__name__)


def sample_construction_year(buildings: DataFrame, end_of_simulation_year: int, construction_year_col: str,
                             random_number_generator: Generator, ):
    random_years = np.full(len(buildings), np.nan)

    # Define class-to-range mapping
    age_class_ranges = {
        "-1919": (1860, 1918),
        "1919-1948": (1919, 1948),
        "1949-1978": (1949, 1978),
        "1979-1990": (1979, 1990),
        "1991-2000": (1991, 2000),
        "2001-2010": (2001, 2010),
        "2011-2019": (2011, 2019),
        "2020-": (2020, end_of_simulation_year),
    }

    # For each class, find matching rows and assign random years
    for age_class, (start, end) in age_class_ranges.items():
        mask = buildings[construction_year_col] == age_class
        count = sum(mask)
        random_years[mask] = random_number_generator.integers(start, end, size=count, endpoint=True)

    return random_years.astype(int)


def simulate_refurbishment(
        df: DataFrame,
        until_year: int,
        parameters: Dict[str, Dict[str, Any]],
        random_number_generator: Generator,
        fill_value: int = 0,
        age_column: str = "age",
        provide_last_refurb_only: bool = False,
) -> DataFrame:
    """
    Simulate component refurbishments by drawing inter-refurbishment intervals from
    user-provided distributions until the given cutoff year.

    parameters format (per component):
      {
        "distribution": callable(gen, params_dict) -> np.ndarray,
        "distribution_parameters": { ... }  # without 'size'; added automatically
      }
    """
    assert age_column in df.columns, (
        f"Column '{age_column}' not in DataFrame, specify the correct column name via the age_column parameter"
    )

    for component, cfg in parameters.items():
        distribution = cfg["distribution"]
        dist_params = dict(cfg["distribution_parameters"])
        dist_params["size"] = df.shape[0]

        refurbishment_offsets = DataFrame(index=df.index)
        n_refurbs = 0

        # Keep sampling while at least one object is still <= until_year
        while any(df[age_column] + refurbishment_offsets.sum(axis=1) <= until_year):
            samples = Series(
                distribution(random_number_generator, dist_params).round().astype(int),
                index=df.index,
                name=f"{component}_{n_refurbs}",
            )
            refurbishment_offsets = pd.concat([refurbishment_offsets, samples], axis=1)
            n_refurbs += 1

        refurb_cum_sum = refurbishment_offsets.cumsum(axis=1).astype(int)
        refurb_years = refurb_cum_sum.add(df[age_column], axis=0)
        refurb_years_masked = refurb_years.mask(refurb_years > until_year, fill_value)

        # Drop columns that are all fill_value
        zero_cols = refurb_years_masked.columns[(refurb_years_masked.T == fill_value).all(axis=1)]
        refurb_years_masked = refurb_years_masked.drop(columns=zero_cols)

        if provide_last_refurb_only:
            # If never refurbished, take the original construction year
            refurb_years_masked = refurb_years_masked.mask(refurb_years_masked == fill_value, df[age_column], axis=0)
            df[component] = refurb_years_masked.max(axis=1)
        else:
            df = pd.concat([df, refurb_years_masked], axis=1)

    return df
