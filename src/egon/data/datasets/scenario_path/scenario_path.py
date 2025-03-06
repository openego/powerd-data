from pathlib import Path
from urllib.request import urlretrieve
import os
import subprocess

import numpy as np
import pandas as pd

from egon.data import config, db
import egon.data.config

sources = egon.data.config.datasets()["scenario_path"]["sources"]

con = db.engine()

scaling_factor = {
    "powerd2025": (2025 - 2019) / (2040 - 2019),
    "powerd2030": (2030 - 2019) / (2040 - 2019),
    "powerd2035": (2035 - 2019) / (2040 - 2019),
}


def clean_existing_scn_path_data():
    scn_path = ["powerd2025", "powerd2030", "powerd2035"]
    # Clean existing data from previous executions
    tables = pd.read_sql(
        """
    SELECT tablename FROM pg_catalog.pg_tables
    WHERE schemaname = 'grid'
    """,
        con,
    )

    tables = tables[
        ~tables["tablename"].isin(
            [
                "egon_etrago_carrier",
                "egon_etrago_temp_resolution",
                "egon_etrago_ac_h2",
                "egon_etrago_hv_busmap",
            ]
        )
    ]

    for scn_path_name in scn_path:
        for table in tables["tablename"]:
            db.execute_sql(
                f"""
            DELETE FROM grid.{table} WHERE scn_name = '{scn_path_name}';
            """
            )
    return


def import_network_structure(scn="powerd2025"):
    scn = "powerd2025"

    # Import buses
    bus = pd.read_sql(
        sql="""
    SELECT * from grid.egon_etrago_bus
    WHERE scn_name = 'eGon100RE'
    """,
        con=con,
    )

    bus["scn_name"] = scn

    bus.to_sql(
        name="egon_etrago_bus",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Import lines
    line = pd.read_sql(
        sql="""
    SELECT * from grid.egon_etrago_line
    WHERE scn_name = 'eGon100RE'
    """,
        con=con,
    )

    line["scn_name"] = scn

    line.to_sql(
        name="egon_etrago_line",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Import transformers
    transformer = pd.read_sql(
        sql="""
    SELECT * from grid.egon_etrago_transformer
    WHERE scn_name = 'eGon100RE'
    """,
        con=con,
    )

    transformer["scn_name"] = scn

    transformer.to_sql(
        name="egon_etrago_transformer",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    return


def load_scn_capacies_link(
    scn1="status2019",
    scn2="eGon100RE",
    scn_path=["powerd2025", "powerd2030", "powerd2035"],
):

    scn1_link = pd.read_sql(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE scn_name = '{scn1}'
        AND bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = '{scn1}'
            )
        AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = '{scn1}'
            )
        """,
        con,
    )

    scn2_link = pd.read_sql(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE scn_name = '{scn2}'
        AND bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = '{scn2}'
            )
        AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = '{scn2}'
            )
        """,
        con,
    )

    scn_capacities = pd.read_sql(
        """
        SELECT * FROM supply.egon_scenario_capacities
        """,
        con,
        index_col="index",
    )

    map_carrier = {
        "urban_central_solar_thermal_collector": "solar_thermal_collector",
        "urban_central_geo_thermal": "geo_thermal",
        "urban_central_gas_boiler": "central_gas_boiler",
        "urban_central_heat_pump": "central_heat_pump",
        "urban_central_resistive_heater": "central_resistive_heater",
        "gas": "OCGT",
    }

    scn_capacities["carrier"] = scn_capacities["carrier"].apply(
        lambda x: map_carrier[x] if x in map_carrier.keys() else x
    )

    carriers_links_from_supply = [
        "central_gas_boiler",
        "central_heat_pump",
        "central_resistive_heater",
        "gas",
        "rural_biomass_boiler",
        "rural_gas_boiler",
        "rural_heat_pump",
        "rural_oil_boiler",
        "rural_resistive_heater",
    ]

    carriers_links = set(
        carriers_links_from_supply
        + list(scn1_link["carrier"])
        + list(scn2_link["carrier"])
    )

    all_scn = [scn1] + scn_path + [scn2]
    link_capacities = pd.DataFrame(index=list(carriers_links), columns=all_scn)
    link_capacities[scn1] = scn1_link.groupby("carrier").p_nom.sum()
    link_capacities[scn2] = scn2_link.groupby("carrier").p_nom.sum()

    for scn in scn_path:
        cap = scn_capacities[scn_capacities["scenario_name"] == scn]
        cap = cap.set_index("carrier")
        link_capacities[scn] = cap["capacity"]

    return link_capacities


def load_scn_capacies_gen(
    scn1="status2019",
    scn2="eGon100RE",
    scn_path=["powerd2025", "powerd2030", "powerd2035"],
):

    scn1_gen = pd.read_sql(
        f"""
        SELECT * FROM grid.egon_etrago_generator
        WHERE scn_name = '{scn1}'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = '{scn1}')
            """,
        con,
    )

    scn2_gen = pd.read_sql(
        f"""
        SELECT * FROM grid.egon_etrago_generator
        WHERE scn_name = '{scn2}'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = '{scn2}')
        """,
        con,
    )

    scn_capacities = pd.read_sql(
        """
        SELECT * FROM supply.egon_scenario_capacities
        """,
        con,
        index_col="index",
    )

    map_carrier = {
        "urban_central_solar_thermal_collector": "solar_thermal_collector",
        "urban_central_geo_thermal": "geo_thermal",
        "urban_central_gas_boiler": "central_gas_boiler",
        "urban_central_heat_pump": "central_heat_pump",
        "urban_central_resistive_heater": "central_resistive_heater",
        "gas": "OCGT",
    }

    scn_capacities["carrier"] = scn_capacities["carrier"].apply(
        lambda x: map_carrier[x] if x in map_carrier.keys() else x
    )

    carriers_gen_from_supply = [
        "oil",
        "solar",
        "solar_rooftop",
        "wind_onshore",
        "lignite",
        "coal",
        "wind_offshore",
        "solar_thermal_collector",
        "geo_thermal",
        "run_of_river",
        "rural_solar_thermal",
        "urban_central_gas_CHP",
        "urban_central_solid_biomass_CHP",
    ]

    carriers_gen = set(
        carriers_gen_from_supply
        + list(scn1_gen["carrier"])
        + list(scn2_gen["carrier"])
    )

    all_scn = [scn1] + scn_path + [scn2]
    gen_capacities = pd.DataFrame(index=list(carriers_gen), columns=all_scn)
    gen_capacities[scn1] = scn1_gen.groupby("carrier").p_nom.sum()
    gen_capacities[scn2] = scn2_gen.groupby("carrier").p_nom.sum()

    for scn in scn_path:
        cap = scn_capacities[scn_capacities["scenario_name"] == scn]
        cap = cap.set_index("carrier")
        gen_capacities[scn] = cap["capacity"]

    return gen_capacities


# load scenarios
def load_scn_no_time_no_foreign(scn_name):
    # load scenario data without timeseries and foreign countries data

    scn_tables = {}
    for t in ["egon_etrago_bus"]:
        scn_tables[t] = pd.read_sql(
            f"""
            SELECT * FROM grid.{t}
            WHERE scn_name = '{scn_name}'
            AND country = 'DE'
            """,
            con,
        )

    for t in [
        "egon_etrago_generator",
        "egon_etrago_load",
        "egon_etrago_storage",
        "egon_etrago_store",
    ]:
        scn_tables[t] = pd.read_sql(
            f"""
            SELECT * FROM grid.{t}
            WHERE scn_name = '{scn_name}'
            AND bus IN (SELECT bus_id from grid.egon_etrago_bus
                        WHERE scn_name = '{scn_name}'
                        AND country = 'DE')
            """,
            con,
        )

    for t in [
        "egon_etrago_line",
        "egon_etrago_link",
        "egon_etrago_transformer",
    ]:
        scn_tables[t] = pd.read_sql(
            f"""
            SELECT * FROM grid.{t}
            WHERE scn_name = '{scn_name}'
            AND bus0 IN (SELECT bus_id from grid.egon_etrago_bus
                        WHERE scn_name = '{scn_name}'
                        AND country = 'DE')
            AND bus1 IN (SELECT bus_id from grid.egon_etrago_bus
                        WHERE scn_name = '{scn_name}'
                        AND country = 'DE')
            """,
            con,
        )

    return scn_tables