from pathlib import Path
from urllib.request import urlretrieve
import os
import subprocess

import numpy as np
import pandas as pd
import pypsa
import geopandas as gpd

from egon.data import config, db
import egon.data.config

from egon.data.datasets.pypsaeur import neighbor_reduction
from egon.data.datasets.scenario_parameters import get_sector_parameters

sources = egon.data.config.datasets()["scenario_path"]["sources"]

con = db.engine()

scaling_factor = {
    "powerd2025": (2025 - 2019) / (2045 - 2019),
    "powerd2030": (2030 - 2019) / (2045 - 2019),
    "powerd2035": (2035 - 2019) / (2045 - 2019),
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


def import_network_structure(scn: str):

    # Import buses
    bus_ac = pd.read_sql(
        sql="""
            SELECT * from grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE' AND carrier = 'AC'
            """,
        con=con,
    )

    other_buses = pd.read_sql(
        sql="""
            SELECT * from grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE' AND carrier <> 'AC' AND country = 'DE'
            """,
        con=con,
    )

    bus = pd.concat([bus_ac, other_buses])

    bus["scn_name"] = scn

    bus.to_sql(
        name="egon_etrago_bus",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Import lines
    line_de = pd.read_sql(
        sql="""
                SELECT * from grid.egon_etrago_line
                WHERE scn_name = 'eGon100RE'
                AND ((bus0 IN (SELECT bus_id FROM grid.egon_etrago_bus
                             WHERE country = 'DE'
                             AND scn_name = 'eGon100RE'))
                     OR (bus1 IN (SELECT bus_id FROM grid.egon_etrago_bus
                                  WHERE country = 'DE'
                                  AND scn_name = 'eGon100RE')))
            """,
        con=con,
        index_col="line_id",
    )
    line_foreign = pd.read_sql(
        sql="""
                SELECT * FROM grid.egon_etrago_line
                WHERE line_id IN (SELECT l.line_id FROM grid.egon_etrago_line l
                JOIN grid.egon_etrago_bus b1 ON l.bus0 = b1.bus_id
                JOIN grid.egon_etrago_bus b2 ON l.bus1 = b2.bus_id
                WHERE b1.country = b2.country
                AND l.scn_name = 'eGon100RE'
                AND b1.country <> 'DE'
                AND b2.country <> 'DE')
                AND scn_name = 'eGon100RE'
            """,
        con=con,
        index_col="line_id",
    )

    line = pd.concat([line_de, line_foreign])
    line.reset_index(inplace=True)
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


def import_efficiency_and_costs(scn):
    target_file = (
        Path(".")
        / "data_bundle_powerd_data"
        / "pypsa_eur"
        / "21122024_3h_clean_run"
        / "results"
        / "postnetworks"
        / f"base_s_39_lc1.25__cb40ex0-T-H-I-B-solar+p3-dist1_{year_scenario[scn]}.nc"
    )
    n = pypsa.Network(target_file)

    buses_de = n.buses[n.buses.country == "DE"]

    gen_de = n.generators[n.generators.bus.isin(buses_de.index)].copy()
    gen_de.carrier = gen_de.carrier.str.replace(" ", "_")
    gen_de.carrier.replace(
        {
            "onwind": "wind_onshore",
            "ror": "run_of_river",
            "offwind-ac": "wind_offshore",
            "offwind-dc": "wind_offshore",
            "offwind-float": "wind_offshore",
            "urban_central_solar_thermal": "urban_central_solar_thermal_collector",
            "residential_rural_solar_thermal": "residential_rural_solar_thermal_collector",
            "services_rural_solar_thermal": "services_rural_solar_thermal_collector",
            "solar-hsat": "solar",
            "urban_central_geo_thermal": "geo_thermal",
        },
        inplace=True,
    )

    gen_cap = pd.DataFrame(
        columns=["p_nom_opt", "marginal_cost", "capital_cost", "efficiency"]
    )
    for g, df in gen_de.groupby("carrier"):
        gen_cap.loc[g, "p_nom_opt"] = df["p_nom_opt"].sum()
        gen_cap.loc[g, "marginal_cost"] = (
            (df["p_nom_opt"] * df["marginal_cost"]) / df["p_nom_opt"].sum()
        ).sum()
        gen_cap.loc[g, "capital_cost"] = (
            (df["p_nom_opt"] * df["capital_cost"]) / df["p_nom_opt"].sum()
        ).sum()
        gen_cap.loc[g, "efficiency"] = (
            (df["p_nom_opt"] * df["efficiency"]) / df["p_nom_opt"].sum()
        ).sum()

    link_de = n.links[
        (n.links.bus0.isin(buses_de.index))
        | (n.links.bus1.isin(buses_de.index))
    ].copy()

    link_de.carrier = link_de.carrier.str.replace(" ", "_")

    link_de.carrier.replace(
        {
            "H2_Electrolysis": "power_to_H2",
            "H2_Fuel_Cell": "H2_to_power",
            "H2_pipeline_retrofitted": "H2_retrofit",
            "SMR": "CH4_to_H2",
            "Sabatier": "H2_to_CH4",
            "gas_for_industry": "CH4_for_industry",
            "gas_pipeline": "CH4",
            "urban_central_gas_boiler": "central_gas_boiler",
            "urban_central_resistive_heater": "central_resistive_heater",
            "urban_central_water_tanks_charger": "central_heat_store_charger",
            "urban_central_water_tanks_discharger": "central_heat_store_discharger",
            "rural_water_tanks_charger": "rural_heat_store_charger",
            "rural_water_tanks_discharger": "rural_heat_store_discharger",
            "urban_central_gas_CHP": "central_gas_CHP",
            "urban_central_air_heat_pump": "central_heat_pump",
            "rural_ground_heat_pump": "rural_heat_pump",
            "CCGT": "OCGT",
        },
        inplace=True,
    )

    link_cap = pd.DataFrame(
        columns=[
            "p_nom_opt",
            "marginal_cost",
            "capital_cost",
            "efficiency",
            "efficiency2",
        ]
    )

    for l, df in link_de.groupby("carrier"):
        link_cap.loc[l, "p_nom_opt"] = df["p_nom_opt"].sum()
        link_cap.loc[l, "marginal_cost"] = (
            (df["p_nom_opt"] * df["marginal_cost"]) / df["p_nom_opt"].sum()
        ).sum()
        link_cap.loc[l, "capital_cost"] = (
            (df["p_nom_opt"] * df["capital_cost"]) / df["p_nom_opt"].sum()
        ).sum()
        link_cap.loc[l, "efficiency"] = (
            (df["p_nom_opt"] * df["efficiency"]) / df["p_nom_opt"].sum()
        ).sum()
        link_cap.loc[l, "efficiency2"] = (
            (df["p_nom_opt"] * df["efficiency2"]) / df["p_nom_opt"].sum()
        ).sum()

    return pd.concat([gen_cap, link_cap])


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


def import_links(scn: str):

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

    cgb_marg_cost = (
        link_cgb1["marginal_cost"].mean()
        + (
            link_cgb2["marginal_cost"].mean()
            - link_cgb1["marginal_cost"].mean()
        )
        * scaling_factor[scn]
    )

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
    link_cgb3["marginal_cost"] = cgb_marg_cost
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
    link_ocgt3 = link_ocgt2.copy().reset_index().set_index("link_id")
    link_ocgt3["scn_name"] = scn
    ocgt_marg_cost = (
        link_ocgt1["marginal_cost"].mean()
        + (
            link_ocgt2["marginal_cost"].mean()
            - link_ocgt1["marginal_cost"].mean()
        )
        * scaling_factor[scn]
    )

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
    link_ocgt3["marginal_cost"] = ocgt_marg_cost
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
    h2_grid3.loc[h2_grid3["build_year"].isna(), "build_year"] = 0
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


def interpolate_marginal_costs(scn):
    marg_cost1 = get_sector_parameters(
        sector="electricity", scenario="status2019"
    )["marginal_cost"]
    marg_cost2 = get_sector_parameters(
        sector="electricity", scenario="eGon100RE"
    )["marginal_cost"]
    marg_cost3 = {}
    for f in marg_cost2.keys():
        marg_cost3[f] = (
            marg_cost1[f]
            + (marg_cost2[f] - marg_cost1[f]) * scaling_factor[scn]
        )

    marg_cost3["run_of_river"] = 0
    marg_cost3["solar_rooftop"] = 0

    return marg_cost3


def import_generators(scn: str):

    cap_gen = load_scn_capacies_gen()
    eff_and_costs = import_efficiency_and_costs(scn)
    marg_cost3 = interpolate_marginal_costs(scn)

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
    geo3["marginal_cost"] = eff_and_costs.loc["geo_thermal", "marginal_cost"]
    geo3["capital_cost"] = eff_and_costs.loc["geo_thermal", "capital_cost"]
    geo3["efficiency"] = eff_and_costs.loc["geo_thermal", "efficiency"]

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

    if len(fossil_carriers) > 0:
        fossil3 = scn1_gen[scn1_gen["carrier"].isin(fossil_carriers)].copy()
        fossil3["scn_name"] = scn

        for fc in fossil_carriers:
            fossil3.loc[fossil3["carrier"] == fc, "marginal_cost"] = (
                marg_cost3[fc]
            )
            fossil3.loc[fossil3["carrier"] == fc, "capital_cost"] = (
                eff_and_costs.loc[fc, "capital_cost"]
            )

        for c, df in fossil3.groupby("carrier"):
            id = df.index
            objective = cap_gen.at[c, scn]
            fossil3.loc[id, "p_nom"] *= (
                objective / fossil3.loc[id, "p_nom"].sum()
            )

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
        gen_var3.loc[df.index, "marginal_cost"] = marg_cost3[c]
        gen_var3.loc[df.index, "capital_cost"] = eff_and_costs.loc[
            c, "capital_cost"
        ]

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
        f"""
        SELECT * FROM grid.egon_etrago_generator_timeseries
        WHERE generator_id IN(
            SELECT generator_id FROM grid.egon_etrago_generator
                WHERE scn_name = 'eGon100RE'
                AND carrier IN {tuple(var_carriers)}
                AND bus IN (
                    SELECT bus_id FROM grid.egon_etrago_bus
                    WHERE country = 'DE'
                    AND scn_name = 'eGon100RE'
                ))
        AND scn_name = 'eGon100RE'
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

    gas3["marginal_cost"] = (
        gas1["marginal_cost"].mean()
        + (gas2["marginal_cost"].mean() - gas1["marginal_cost"].mean())
        * scaling_factor[scn]
    )

    gas3.to_sql(
        name="egon_etrago_generator",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )
    return


def import_loads(scn: str):

    scn1_load = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_load
        WHERE scn_name = 'status2019'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'status2019'
            )
        """,
        con,
        index_col="load_id",
    )

    scn2_load = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_load
        WHERE scn_name = 'eGon100RE'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
            )
        """,
        con,
        index_col="load_id",
    )

    scn3_load = scn2_load.copy()

    scn1_load_t = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_load_timeseries
        WHERE scn_name = 'status2019'
        AND load_id IN (
            SELECT load_id from grid.egon_etrago_load
            WHERE bus IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country = 'DE'
                AND scn_name = 'status2019'
                )
            AND scn_name = 'status2019')
        """,
        con,
        index_col="load_id",
    )

    scn2_load_t = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_load_timeseries
        WHERE scn_name = 'eGon100RE'
        AND load_id IN (
            SELECT load_id from grid.egon_etrago_load
            WHERE bus IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country = 'DE'
                AND scn_name = 'eGon100RE'
                )
            AND scn_name = 'eGon100RE')
        """,
        con,
        index_col="load_id",
    )

    # dealing with loads only present in eGon100RE
    only100_carrier = [
        "O2",
        "H2_hgv_load",
        "H2_for_industry",
        "CH4_for_industry",
    ]
    only100 = scn2_load[scn2_load["carrier"].isin(only100_carrier)]

    for c, df in only100.groupby("carrier"):
        a = scn2_load_t.loc[df.index, "p_set"].apply(
            lambda x: np.array(x).sum()
        )
        print(f"{c}:{a.sum()}")

    for c, df in only100.groupby("carrier"):
        scn2_load_t.loc[df.index, "p_set"] = scn2_load_t.loc[
            df.index, "p_set"
        ].apply(lambda x: np.array(x) * scaling_factor[scn])

    for c, df in only100.groupby("carrier"):
        a = scn2_load_t.loc[df.index, "p_set"].apply(
            lambda x: np.array(x).sum()
        )
        print(f"{c}:{a.sum()}")

    # Dealing with land_transport_EV loads
    evl1 = scn1_load[scn1_load["carrier"] == "land_transport_EV"].copy()
    evl2 = scn2_load[scn2_load["carrier"] == "land_transport_EV"].copy()
    evl1_total = (
        scn1_load_t.loc[evl1.index, "p_set"]
        .apply(lambda x: np.array(x).sum())
        .sum()
    )
    evl2_total = (
        scn2_load_t.loc[evl2.index, "p_set"]
        .apply(lambda x: np.array(x).sum())
        .sum()
    )

    objective = evl1_total + (evl2_total - evl1_total) * scaling_factor[scn]

    scn2_load_t.loc[evl2.index, "p_set"] = scn2_load_t.loc[
        evl2.index, "p_set"
    ].apply(lambda x: np.array(x) * objective / evl2_total)

    # Dealing with AC loads
    ac_load = pd.concat(
        [
            scn1_load[scn1_load["carrier"] == "AC"],
            scn2_load[scn2_load["carrier"] == "AC"],
        ]
    ).copy()

    for b, df in ac_load.groupby("bus"):
        df1 = df[df["scn_name"] == "status2019"]
        df2 = df[df["scn_name"] == "eGon100RE"]
        df1_total = (
            scn1_load_t.loc[df1.index, "p_set"]
            .apply(lambda x: np.array(x).sum())
            .sum()
        )
        df2_total = (
            scn2_load_t.loc[df2.index, "p_set"]
            .apply(lambda x: np.array(x).sum())
            .sum()
        )
        objective = df1_total + (df2_total - df1_total) * scaling_factor[scn]
        scn2_load_t.loc[df2.index, "p_set"] = scn2_load_t.loc[
            df2.index, "p_set"
        ].apply(lambda x: np.array(x) * objective / df2_total)

    # Dealing with rural_heat loads
    rh1 = scn1_load[scn1_load["carrier"] == "rural_heat"].copy()
    rh2 = scn2_load[scn2_load["carrier"] == "rural_heat"].copy()
    rh1_total = (
        scn1_load_t.loc[rh1.index, "p_set"]
        .apply(lambda x: np.array(x).sum())
        .sum()
    )
    rh2_total = (
        scn2_load_t.loc[rh2.index, "p_set"]
        .apply(lambda x: np.array(x).sum())
        .sum()
    )

    objective = rh1_total + (rh2_total - rh1_total) * scaling_factor[scn]

    scn2_load_t.loc[rh2.index, "p_set"] = scn2_load_t.loc[
        rh2.index, "p_set"
    ].apply(lambda x: np.array(x) * objective / rh2_total)

    # Dealing with central_heat loads
    ch1 = scn1_load[scn1_load["carrier"] == "central_heat"].copy()
    ch2 = scn2_load[scn2_load["carrier"] == "central_heat"].copy()
    ch1_total = (
        scn1_load_t.loc[ch1.index, "p_set"]
        .apply(lambda x: np.array(x).sum())
        .sum()
    )
    ch2_total = (
        scn2_load_t.loc[ch2.index, "p_set"]
        .apply(lambda x: np.array(x).sum())
        .sum()
    )

    objective = ch1_total + (ch2_total - ch1_total) * scaling_factor[scn]

    scn2_load_t.loc[ch2.index, "p_set"] = scn2_load_t.loc[
        ch2.index, "p_set"
    ].apply(lambda x: np.array(x) * objective / ch2_total)

    scn3_load["scn_name"] = scn
    scn3_load.reset_index(inplace=True)
    scn3_load.to_sql(
        name="egon_etrago_load",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )
    scn2_load_t["p_set"] = scn2_load_t["p_set"].apply(list)
    scn2_load_t["scn_name"] = scn
    scn2_load_t.reset_index(inplace=True)
    scn2_load_t.to_sql(
        name="egon_etrago_load_timeseries",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )


def import_storage_units(scn: str):

    scn1_su = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_storage
        WHERE scn_name = 'status2019'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'status2019'
            )
        """,
        con,
    )

    scn2_su = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_storage
        WHERE scn_name = 'eGon100RE'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
            )
        """,
        con,
    )

    # Dealing with battery
    battery3 = scn2_su[scn2_su["carrier"] == "battery"].copy()
    battery3["scn_name"] = scn

    battery3.to_sql(
        name="egon_etrago_storage",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    # Dealing with pumped_hydro
    capacity_ph = pd.read_sql(
        f"""
            SELECT capacity FROM supply.egon_scenario_capacities
            WHERE carrier = 'pumped_hydro' AND scenario_name = '{scn}'
            """,
        con,
    ).iat[0, 0]

    ph1 = scn1_su[scn1_su["carrier"] == "pumped_hydro"]
    ph2 = scn2_su[scn2_su["carrier"] == "pumped_hydro"]
    ph3 = ph2.copy()

    ph3["p_nom"] *= capacity_ph / ph3["p_nom"].sum()

    ph3["scn_name"] = scn
    ph3.to_sql(
        name="egon_etrago_storage",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    return


def import_stores(scn: str):
    scn1_store = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_store
        WHERE scn_name = 'status2019'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'status2019'
            )
        """,
        con,
    )

    scn2_store = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_store
        WHERE scn_name = 'eGon100RE'
        AND bus IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE country = 'DE'
            AND scn_name = 'eGon100RE'
            )
        """,
        con,
    )

    scn3_store = scn2_store.copy()

    scn2_store_t = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_store_timeseries
        WHERE scn_name = 'eGon100RE'
        AND store_id IN (
            SELECT store_id from grid.egon_etrago_store
            WHERE bus IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country = 'DE'
                AND scn_name = 'eGon100RE'
                )
            AND scn_name = 'eGon100RE')
        """,
        con,
        index_col="store_id",
    )

    all_stores = pd.concat([scn1_store, scn2_store]).copy()

    # dealing with stores only present in eGon100RE
    only100_carrier = [
        "central_heat_store",
        "rural_heat_store",
        "battery_storage",
        "H2_overground",
        "CH4",
        "H2_underground",
    ]
    only100_store = scn2_store[scn2_store["carrier"].isin(only100_carrier)]
    for c, df in only100_store.groupby("carrier"):
        scn3_store.loc[df.index, "e_nom"] *= scaling_factor[scn]

    # Dealing with DSM
    dsm = all_stores[all_stores["carrier"] == "dsm"].copy()
    dsm_buses = pd.read_sql(
        """
        SELECT * FROM grid.egon_etrago_bus
        WHERE scn_name IN ('eGon100RE', 'status2019')
        AND carrier = 'dsm'
        """,
        con,
        index_col="bus_id",
    )
    map_scn1_to_scn2 = {}
    for b, df in dsm_buses.groupby(["x", "y"]):
        bus1 = df.index[df["scn_name"] == "status2019"][0]
        bus2 = df.index[df["scn_name"] == "eGon100RE"][0]
        map_scn1_to_scn2[bus1] = bus2

    dsm["bus"] = dsm["bus"].apply(
        lambda x: map_scn1_to_scn2[x] if x in map_scn1_to_scn2.keys() else x
    )

    for b, df in dsm.groupby("bus"):
        init = df["e_nom"][df["scn_name"] == "status2019"].sum()
        final = df["e_nom"][df["scn_name"] == "eGon100RE"].sum()
        objective = init + (final - init) * scaling_factor[scn]
        scn3_store.loc[df.index, "e_nom"] *= (
            objective / scn3_store.loc[df.index, "e_nom"].sum()
        )

    scn3_store["scn_name"] = scn
    scn3_store.to_sql(
        name="egon_etrago_store",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )

    scn2_store_t["scn_name"] = scn
    scn2_store_t.reset_index(inplace=True)
    scn2_store_t.to_sql(
        name="egon_etrago_store_timeseries",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )
    return


def import_foreign(scn_name, year):
    neighbor_reduction(scn_name, year)

    # import links joining DE and foreign countries
    link_foreign = pd.read_sql(
        sql="""
                SELECT * from grid.egon_etrago_link
                WHERE scn_name = 'eGon100RE'
                AND ((bus0 IN (SELECT bus_id FROM grid.egon_etrago_bus
                             WHERE country = 'DE'
                             AND scn_name = 'eGon100RE'))
                     AND (bus1 IN (SELECT bus_id FROM grid.egon_etrago_bus
                                  WHERE country <> 'DE'
                                  AND scn_name = 'eGon100RE')))
                OR ((bus0 IN (SELECT bus_id FROM grid.egon_etrago_bus
                             WHERE country <> 'DE'
                             AND scn_name = 'eGon100RE'))
                     AND (bus1 IN (SELECT bus_id FROM grid.egon_etrago_bus
                                  WHERE country = 'DE'
                                  AND scn_name = 'eGon100RE')))
            """,
        con=con,
        index_col="link_id",
    )

    bus_ch4_h2 = pd.read_sql(
        sql=f"""
                SELECT * from grid.egon_etrago_bus
                WHERE scn_name IN ('eGon100RE', '{scn_name}')
                AND carrier IN ('CH4', 'H2')
                AND country <> 'DE'
            """,
        con=con,
        index_col="bus_id",
    )

    map_scn1_to_scn2 = {}
    for b, df in bus_ch4_h2.groupby(["x", "y", "carrier"]):
        bus1 = df.index[df["scn_name"] == "eGon100RE"][0]
        bus2 = df.index[df["scn_name"] == scn_name][0]
        map_scn1_to_scn2[bus1] = bus2

    link_foreign["bus0"] = link_foreign["bus0"].apply(
        lambda x: map_scn1_to_scn2[x] if x in map_scn1_to_scn2.keys() else x
    )

    link_foreign["bus1"] = link_foreign["bus1"].apply(
        lambda x: map_scn1_to_scn2[x] if x in map_scn1_to_scn2.keys() else x
    )

    link_foreign["scn_name"] = scn_name
    link_foreign.reset_index(inplace=True)
    link_foreign.to_sql(
        name="egon_etrago_link",
        con=con,
        schema="grid",
        if_exists="append",
        index=False,
    )
