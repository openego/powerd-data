from pathlib import Path
from urllib.request import urlretrieve
import os
import subprocess

import numpy as np
import pandas as pd
import geopandas as gpd

from egon.data import config, db
import egon.data.config

sources = egon.data.config.datasets()["scenario_path"]["sources"]

con = db.engine()

scaling_factor = {
    "powerd2025": (2025 - 2019) / (2040 - 2019),
    "powerd2030": (2030 - 2019) / (2040 - 2019),
    "powerd2035": (2035 - 2019) / (2040 - 2019),
}

year_scenario = {
    "powerd2025": 2025,
    "powerd2030": 2030,
    "powerd2035": 2035,
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


def import_links(scn="powerd2025"):
    scn = "powerd2025"
    cap_link = load_scn_capacies_link()

    scn1_link = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_link
        WHERE scn_name = 'status2019'
        AND bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'status2019'
            )
        AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'status2019'
            )
        """,
        con,
    )

    scn2_link = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_link
        WHERE scn_name = 'eGon100RE'
        AND bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
            )
        AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
            )
        """,
        con,
    )

    # Dealing with dsm
    dsm1 = scn1_link[scn1_link["carrier"] == "dsm"].set_index("bus0").copy()
    dsm2 = scn2_link[scn2_link["carrier"] == "dsm"].set_index("bus0").copy()
    dsm3 = dsm2.copy()
    dsm3["scn_name"] = scn
    dsm3["p_nom"] = (
        dsm1["p_nom"] + (dsm2["p_nom"] - dsm1["p_nom"]) * scaling_factor[scn]
    )
    dsm3.reset_index(inplace=True)
    dsm3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    dsm3_t = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_link_timeseries
        WHERE link_id IN(
        SELECT link_id FROM grid.egon_etrago_link
        WHERE bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
        )
        AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
        )
        AND carrier = 'dsm')
        """,
        con,
    )

    dsm3_t["scn_name"] = scn

    dsm3_t.to_sql(
        name="egon_etrago_link_timeseries",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # dealing with rural_heat_pump
    link_rhp1 = (
        scn1_link[scn1_link["carrier"].isin(["rural_heat_pump"])]
        .copy()
        .set_index("bus0")
    )
    link_rhp2 = (
        scn2_link[scn2_link["carrier"].isin(["rural_heat_pump"])]
        .copy()
        .set_index("bus0")
    )
    link_rhp3 = link_rhp2.copy()
    link_rhp3["scn_name"] = scn

    link_rhp3["p_nom"] = (
        link_rhp1["p_nom"]
        + (link_rhp2["p_nom"] - link_rhp1["p_nom"]) * scaling_factor[scn]
    )
    factor_to_pypsaeur = (
        cap_link.at["rural_heat_pump", scn] / link_rhp3["p_nom"].sum()
    )
    link_rhp3["p_nom"] = link_rhp3["p_nom"] * factor_to_pypsaeur

    link_rhp3.reset_index(inplace=True)
    link_rhp3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    link_rhp3_t = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_link_timeseries
        WHERE link_id IN(
        SELECT link_id FROM grid.egon_etrago_link
        WHERE bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
        )
        AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
        )
        AND carrier = 'rural_heat_pump')
        """,
        con,
    )

    link_rhp3_t["scn_name"] = scn

    link_rhp3_t.to_sql(
        name="egon_etrago_link_timeseries",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Dealing with central_gas_boiler
    link_cgb1 = (
        scn1_link[scn1_link["carrier"].isin(["central_gas_boiler"])]
        .copy()
        .set_index("bus1")
    )
    link_cgb2 = (
        scn2_link[scn2_link["carrier"].isin(["central_gas_boiler"])]
        .copy()
        .set_index("bus1")
    )
    link_cgb3 = link_cgb2.copy()
    link_cgb3["scn_name"] = scn

    cgb1_geo = gpd.read_postgis(
        """
            SELECT bus_id, geom FROM grid.egon_etrago_bus
            WHERE scn_name = 'status2019'
            AND carrier = 'central_heat'
            """,
        con,
        geom_col="geom",
    ).set_index("bus_id")

    cgb2_geo = gpd.read_postgis(
        """
            SELECT bus_id, geom FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE'
            AND carrier = 'central_heat'
            """,
        con,
        geom_col="geom",
    ).set_index("bus_id")
    cgb1_to_cgb2 = {}
    for g in cgb1_geo.index:
        dist = cgb2_geo.distance(cgb1_geo["geom"][g])
        dist.sort_values(inplace=True)
        for d in dist.index:
            if d not in cgb1_to_cgb2.values():
                cgb1_to_cgb2[g] = d
                break

    link_cgb1.index = link_cgb1.index.map(cgb1_to_cgb2)
    missing_cgb = pd.DataFrame(
        0,
        index=link_cgb2.index[~link_cgb2.index.isin(link_cgb1.index)],
        columns=["p_nom"],
    )
    link_cgb1 = pd.concat([link_cgb1, missing_cgb])

    link_cgb3["p_nom"] = (
        link_cgb1["p_nom"]
        + (link_cgb2["p_nom"] - link_cgb1["p_nom"]) * scaling_factor[scn]
    )
    factor_to_pypsaeur = (
        cap_link.at["central_gas_boiler", scn] / link_cgb3["p_nom"].sum()
    )
    link_cgb3["p_nom"] = link_cgb3["p_nom"] * factor_to_pypsaeur

    link_cgb3.reset_index(inplace=True)
    link_cgb3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # dealing with OCGT
    link_ocgt1 = (
        scn1_link[scn1_link["carrier"].isin(["OCGT"])].copy().set_index("bus1")
    )
    link_ocgt2 = (
        scn2_link[scn2_link["carrier"].isin(["OCGT"])].copy().set_index("bus1")
    )
    link_ocgt3 = link_ocgt2.copy().set_index("link_id")
    link_ocgt3["scn_name"] = scn

    not_in_ocgt2 = link_ocgt1[
        ~link_ocgt1.index.isin(link_ocgt2.index.unique())
    ]
    ac_geo = gpd.read_postgis(
        """
            SELECT bus_id, geom FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE'
            AND carrier = 'AC'
            """,
        con,
        geom_col="geom",
    ).set_index("bus_id")
    ac_ocgt1_not_in_ocgt2 = ac_geo[ac_geo.index.isin(list(not_in_ocgt2.index))]
    ac_ocgt2 = ac_geo[ac_geo.index.isin(list(link_ocgt2.index))]

    ocgt1_to_ocgt2 = {}
    for l in not_in_ocgt2.index.unique():
        dist = ac_ocgt2.distance(ac_ocgt1_not_in_ocgt2["geom"][l])
        dist.sort_values(inplace=True)
        ocgt1_to_ocgt2[l] = dist.index[0]

    link_ocgt1.reset_index(inplace=True)
    link_ocgt1["bus1"] = link_ocgt1["bus1"].apply(
        lambda x: x if x not in ocgt1_to_ocgt2.keys() else ocgt1_to_ocgt2[x]
    )
    link_ocgt2.reset_index(inplace=True)
    for b, df in link_ocgt2.groupby("bus1"):
        ids = df.link_id
        ini = link_ocgt1[link_ocgt1["bus1"] == b]["p_nom"].sum()
        fin = df["p_nom"].sum()
        factor_bus = (ini + (fin - ini) * scaling_factor[scn]) / fin
        link_ocgt3.loc[ids, "p_nom"] = (
            link_ocgt3.loc[ids, "p_nom"] * factor_bus
        )

    factor_to_pypsaeur = cap_link.at["OCGT", scn] / link_ocgt3["p_nom"].sum()
    link_ocgt3["p_nom"] *= factor_to_pypsaeur

    link_ocgt3.reset_index(inplace=True)
    link_ocgt3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # dealing with H2_grid
    h2_grid2 = scn2_link[scn2_link["carrier"] == "H2_grid"].copy()
    h2_grid3 = h2_grid2.copy()
    h2_grid3["scn_name"] = scn
    h2_grid3 = h2_grid3[h2_grid3["build_year"] <= year_scenario[scn]]

    h2_grid3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # dealing with identical links as in eGon100RE
    identical = [
        "CH4",
        "CH4_to_H2",
        "H2_saltcavern",
        "H2_to_CH4",
        "H2_to_power",
        "PtH2_O2",
        "PtH2_waste_heat",
        "rural_heat_store_charger",
        "rural_heat_store_discharger",
        "power_to_H2",
        "rural_heat_store_charger",
        "rural_heat_store_discharger",
        "BEV_charger",
    ]
    identical3 = scn2_link[scn2_link["carrier"].isin(identical)].copy()
    identical3["scn_name"] = scn

    identical3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # dealing with carriers which are only present in eGon100RE and scn path.
    # This links are only scaled
    scale_carriers = [
        "rural_resistive_heater",
        "rural_gas_boiler",
        "central_resistive_heater",
    ]
    scale3 = scn2_link[scn2_link["carrier"].isin(scale_carriers)].copy()
    scale3["scn_name"] = scn

    for c, df in scale3.groupby("carrier"):
        id = df.index
        objective = cap_link.at[c, scn]
        scale3.loc[id, "p_nom"] *= objective / scale3.loc[id, "p_nom"].sum()

    scale3.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    return


###############################################################################
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


def import_generators(scn="powerd2025"):
    scn = "powerd2025"
    cap_gen = load_scn_capacies_gen()

    scn1_gen = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_generator
        WHERE scn_name = 'status2019'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'status2019'
            )
        """,
        con,
    )

    scn2_gen = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_generator
        WHERE scn_name = 'eGon100RE'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
            )
        """,
        con,
    )

    # Dealing with geo_thermal
    geo3 = scn2_gen[scn2_gen["carrier"] == "geo_thermal"].copy()
    geo3["scn_name"] = scn

    objective = cap_gen.at["geo_thermal", scn]
    geo3["p_nom"] *= objective / geo3["p_nom"].sum()

    geo3.to_sql(
        name="egon_etrago_generator",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Dealing with oil, coal and lignite

    fossil_carriers = [
        "oil",
        "coal",
        "lignite",
    ]

    fossil_carriers = cap_gen.loc[fossil_carriers, scn][
        ~cap_gen.loc[fossil_carriers, scn].isna()
    ].index.values

    if len(fossil_carriers) == 0:
        return

    fossil3 = scn1_gen[scn1_gen["carrier"].isin(fossil_carriers)].copy()
    fossil3["scn_name"] = scn

    for c, df in fossil3.groupby("carrier"):
        id = df.index
        objective = cap_gen.at[c, scn]
        fossil3.loc[id, "p_nom"] *= objective / fossil3.loc[id, "p_nom"].sum()

    fossil3.to_sql(
        name="egon_etrago_gen",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Dialing with run_of_river, solar_rooftop, wind_offshore, wind_onshore
    # and solar
    var_carriers = [
        "run_of_river",
        "solar_rooftop",
        "wind_offshore",
        "wind_onshore",
        "solar",
    ]
    gen_var1 = (
        scn1_gen[scn1_gen["carrier"].isin(var_carriers)]
        .copy()
        .set_index("generator_id")
    )
    gen_var2 = (
        scn2_gen[scn2_gen["carrier"].isin(var_carriers)]
        .copy()
        .set_index("generator_id")
    )
    gen_var3 = gen_var2.copy()
    gen_var3["scn_name"] = scn

    total_gen_var = pd.concat([gen_var1, gen_var2]).copy()

    new_gen_from_2019 = []
    for c, df in total_gen_var.groupby(["carrier", "bus"]):
        df1 = df[df["scn_name"] == "status2019"]
        df2 = df[df["scn_name"] == "eGon100RE"]
        init = df1.p_nom.sum()
        final = df2.p_nom.sum()

        if (init != 0) & (final != 0):
            ids = df2.index
            gen_var3.loc[ids, "p_nom"] = gen_var3.loc[ids, "p_nom"] - (
                (final - init) * scaling_factor[scn]
            )

        elif final == 0:
            new_gen = df1.copy()
            [new_gen_from_2019.append(x) for x in list(new_gen.index)]
            new_gen["scn_name"] = scn
            new_gen["p_nom"] *= scaling_factor[scn]
            gen_var3 = pd.concat([gen_var3, new_gen])

    for c, df in gen_var3.groupby("carrier"):
        factor_to_pypsaeur = cap_gen.at[c, scn] / df["p_nom"].sum()
        gen_var3.loc[df.index, "p_nom"] *= factor_to_pypsaeur

    gen_var3.reset_index(inplace=True)
    gen_var3.to_sql(
        name="egon_etrago_generator",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    gen_var3_t_2019 = pd.read_sql(
        f"""
        SELECT * FROM grid.egon_etrago_generator_timeseries
        WHERE generator_id IN {tuple(new_gen_from_2019)}
        AND scn_name = 'status2019'
        """,
        con,
    )

    gen_var3_t_100RE = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_generator_timeseries
        WHERE generator_id IN(
        SELECT generator_id FROM grid.egon_etrago_generator
        WHERE bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
        )
        AND scn_name = 'eGon100RE')
        """,
        con,
    )

    gen_var3_t = pd.concat([gen_var3_t_2019, gen_var3_t_100RE])
    gen_var3_t["scn_name"] = scn

    gen_var3_t.to_sql(
        name="egon_etrago_generator_timeseries",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Dealing with O2
    oxy3 = scn2_gen[scn2_gen["carrier"] == "O2"].copy()
    oxy3["scn_name"] = scn

    oxy3.to_sql(
        name="egon_etrago_generator",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Dealing with CH4
    gas1 = scn1_gen[scn1_gen["carrier"] == "CH4"].copy()
    gas2 = scn2_gen[scn2_gen["carrier"] == "CH4"].copy()
    gas3 = scn2_gen[scn2_gen["carrier"] == "CH4"].copy()
    gas3["scn_name"] = scn

    obj = (
        gas1["p_nom"].sum()
        + (gas2["p_nom"].sum() - gas1["p_nom"].sum()) * scaling_factor[scn]
    )
    gas3["p_nom"] *= obj / gas3["p_nom"].sum()

    gas3.to_sql(
        name="egon_etrago_generator",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )
    return


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
