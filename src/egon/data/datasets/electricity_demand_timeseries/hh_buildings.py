"""
Household electricity demand time series for scenarios in 2035 and 2050
assigned to OSM-buildings.

Assignment of household electricity demand timeseries to OSM buildings and
generation of randomly placed synthetic 5x5m buildings if no sufficient OSM-data
available in the respective cencus cell.

The resulting data is stored in separate tables

* `openstreetmap.osm_buildings_synthetic`:
    Lists generated synthetic building with id and cell_id
* `demand.egon_household_electricity_profile_of_buildings`:
    Mapping of demand timeseries and buildings including cell_id, building
    area and peak load

Both tables are created within :func:`map_houseprofiles_to_buildings`.


**The following datasets from the database are used for creation:**

* `demand.household_electricity_profiles_in_census_cells`:
    Lists references and scaling parameters to time series data for each
    household in a cell by identifiers. This table is fundamental for creating
    subsequent data like demand profiles on MV grid level or for determining
    the peak load at load. Only the profile reference and the cell identifiers
    are used.

* `society.egon_destatis_zensus_apartment_building_population_per_ha`:
    Lists number of apartments, buildings and population for each census cell.

* `boundaries.egon_map_zensus_buildings_residential`:
    List of OSM tagged buildings which are considered to be residential.


**What is the goal?**

To assign every household demand timeseries, which already exist at cell level,
to a specific OSM building.

**What is the challenge?**

The census and the OSM dataset differ from each other. The census uses
statistical methods and therefore lacks accuracy at high spatial resolution.
The OSM datasets is community based dataset which is extended throughout and
does not claim to be complete. By merging these datasets inconsistencies need
to be addressed. For example: not yet tagged buildings in OSM or new building
areas not considered in census 2011.

**How are these datasets combined?**

The assignment of household demand timeseries to buildings takes place at cell
level. Within each cell a pool of profiles exists, produced by the 'HH Demand"
module. These profiles are randomly assigned to a filtered list of OSM buildings
within this cell. Every profile is assigned to a building and every building
get a profile assigned if there is enough households by the census data. If
there are more profiles then buildings, all additional profiles are randomly
assigned. Therefore multiple profiles can be assigned to one building, making
it a multi-household building.


**What are central assumptions during the data processing?**

* Mapping zensus data to OSM data is not trivial. Discrepancies are substituted.
* Missing OSM buildings are generated by census building count.
* If no census building count data is available, the number of buildings is
derived by an average rate of households/buildings applied to the number of
households.

**Drawbacks and limitations of the data**

* Missing OSM buildings in cells without census building count are derived by
an average rate of households/buildings applied to the number of households.
As only whole houses can exist, the substitute is ceiled to the next higher
integer. Ceiling is applied to avoid rounding to amount of 0 buildings.

* As this datasets is a cascade after profile assignement at census cells
also check drawbacks and limitations in hh_profiles.py.



Example Query
-----


* Get a list with number of houses, households and household types per census cell

.. code-block:: SQL

    SELECT t1.cell_id, building_count, hh_count, hh_types
        FROM(
            SELECT cell_id, Count(distinct(building_id)) as building_count,
            count(profile_id) as hh_count
                FROM demand.egon_household_electricity_profile_of_buildings
            Group By cell_id
        ) as t1
    FULL OUTER JOIN(
        SELECT cell_id, array_agg(array[cast(hh_10types as char),
         hh_type]) as hh_types
        FROM society.egon_destatis_zensus_household_per_ha_refined
        GROUP BY cell_id
        ) as t2
    ON t1.cell_id = t2.cell_id


Notes
-----

This module docstring is rather a dataset documentation. Once, a decision
is made in ... the content of this module docstring needs to be moved to
docs attribute of the respective dataset class.
"""
from functools import partial
import random

from geoalchemy2 import Geometry
from sqlalchemy import REAL, Column, Integer, String, Table, func, inspect
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand_timeseries.hh_profiles import (
    HouseholdElectricityProfilesInCensusCells,
    get_iee_hh_demand_profiles_raw,
)
from egon.data.datasets.electricity_demand_timeseries.tools import (
    random_point_in_square,
)
import egon.data.config

engine = db.engine()
Base = declarative_base()

data_config = egon.data.config.datasets()
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]
np.random.seed(RANDOM_SEED)


class HouseholdElectricityProfilesOfBuildings(Base):
    __tablename__ = "egon_household_electricity_profile_of_buildings"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, index=True)
    cell_id = Column(Integer, index=True)
    profile_id = Column(String, index=True)


class OsmBuildingsSynthetic(Base):
    __tablename__ = "osm_buildings_synthetic"
    __table_args__ = {"schema": "openstreetmap"}

    id = Column(String, primary_key=True)
    cell_id = Column(String, index=True)
    geom_building = Column(Geometry("Polygon", 3035), index=True)
    geom_point = Column(Geometry("POINT", 3035))
    n_amenities_inside = Column(Integer)
    building = Column(String(11))
    area = Column(REAL)


class BuildingElectricityPeakLoads(Base):
    __tablename__ = "egon_building_electricity_peak_loads"
    __table_args__ = {"schema": "demand"}

    building_id = Column(Integer, primary_key=True)
    scenario = Column(String, primary_key=True)
    sector = Column(String, primary_key=True)
    peak_load_in_w = Column(REAL)
    voltage_level = Column(Integer, index=True)


def match_osm_and_zensus_data(
    egon_hh_profile_in_zensus_cell,
    egon_map_zensus_buildings_residential,
):
    """
    Compares OSM buildings and census hh demand profiles.

    OSM building data and hh demand profiles based on census data is compared.
    Census cells with only profiles but no osm-ids are identified to generate
    synthetic buildings. Census building count is used, if available, to define
    number of missing buildings. Otherwise, the overall mean profile/building
    rate is used to derive the number of buildings from the number of already
    generated demand profiles.

    Parameters
    ----------
    egon_hh_profile_in_zensus_cell: pd.DataFrame
        Table mapping hh demand profiles to census cells

    egon_map_zensus_buildings_residential: pd.DataFrame
        Table with buildings osm-id and cell_id

    Returns
    -------
    pd.DataFrame
        Table with cell_ids and number of missing buildings
    """
    # count number of profiles for each cell
    profiles_per_cell = egon_hh_profile_in_zensus_cell.cell_profile_ids.apply(
        len
    )

    # Add number of profiles per cell
    number_of_buildings_profiles_per_cell = pd.merge(
        left=profiles_per_cell,
        right=egon_hh_profile_in_zensus_cell["cell_id"],
        left_index=True,
        right_index=True,
    )

    # count buildings/ids for each cell
    buildings_per_cell = egon_map_zensus_buildings_residential.groupby(
        "cell_id"
    )["id"].count()
    buildings_per_cell = buildings_per_cell.rename("building_ids")

    # add buildings left join to have all the cells with assigned profiles
    number_of_buildings_profiles_per_cell = pd.merge(
        left=number_of_buildings_profiles_per_cell,
        right=buildings_per_cell,
        left_on="cell_id",
        right_index=True,
        how="left",
    )

    # identify cell ids with profiles but no buildings
    number_of_buildings_profiles_per_cell = (
        number_of_buildings_profiles_per_cell.fillna(0).astype(int)
    )
    missing_buildings = number_of_buildings_profiles_per_cell.loc[
        number_of_buildings_profiles_per_cell.building_ids == 0,
        ["cell_id", "cell_profile_ids"],
    ].set_index("cell_id")

    # query zensus building count
    egon_destatis_building_count = Table(
        "egon_destatis_zensus_apartment_building_population_per_ha",
        Base.metadata,
        schema="society",
    )
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(egon_destatis_building_count, None)

    with db.session_scope() as session:
        cells_query = session.query(
            egon_destatis_building_count.c.zensus_population_id,
            egon_destatis_building_count.c.building_count,
        )

    egon_destatis_building_count = pd.read_sql(
        cells_query.statement,
        cells_query.session.bind,
        index_col="zensus_population_id",
    )
    egon_destatis_building_count = egon_destatis_building_count.dropna()

    missing_buildings = pd.merge(
        left=missing_buildings,
        right=egon_destatis_building_count,
        left_index=True,
        right_index=True,
        how="left",
    )

    # exclude cells without buildings
    only_cells_with_buildings = (
        number_of_buildings_profiles_per_cell["building_ids"] != 0
    )
    # get profile/building rate for each cell
    profile_building_rate = (
        number_of_buildings_profiles_per_cell.loc[
            only_cells_with_buildings, "cell_profile_ids"
        ]
        / number_of_buildings_profiles_per_cell.loc[
            only_cells_with_buildings, "building_ids"
        ]
    )

    # prepare values for missing building counts by number of profile ids
    building_count_fillna = missing_buildings.loc[
        missing_buildings["building_count"].isna(), "cell_profile_ids"
    ]
    # devide by median profile/building rate
    building_count_fillna = (
        building_count_fillna / profile_building_rate.median()
    )
    # replace missing building counts
    missing_buildings["building_count"] = missing_buildings[
        "building_count"
    ].fillna(value=building_count_fillna)

    # ceil to have at least one building each cell and make type int
    missing_buildings = missing_buildings.apply(np.ceil).astype(int)
    # generate list of building ids for each cell
    missing_buildings["building_count"] = missing_buildings[
        "building_count"
    ].apply(range)
    missing_buildings = missing_buildings.explode(column="building_count")

    return missing_buildings


def generate_synthetic_buildings(missing_buildings, edge_length):
    """
    Generate synthetic square buildings in census cells for every entry
    in missing_buildings.

    Generate random placed synthetic buildings incl geom data within the bounds
    of the cencus cell. Buildings have each a square area with edge_length^2.


    Parameters
    ----------
    missing_buildings: pd.Series or pd.DataFrame
        Table with cell_ids and building number
    edge_length: int
        Edge length of square synthetic building in meter

    Returns
    -------
    pd.DataFrame
        Table with generated synthetic buildings, area, cell_id and geom data

    """
    destatis_zensus_population_per_ha_inside_germany = Table(
        "destatis_zensus_population_per_ha_inside_germany",
        Base.metadata,
        schema="society",
    )
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(
        destatis_zensus_population_per_ha_inside_germany, None
    )

    with db.session_scope() as session:
        cells_query = session.query(
            destatis_zensus_population_per_ha_inside_germany
        ).filter(
            destatis_zensus_population_per_ha_inside_germany.c.id.in_(
                missing_buildings.index
            )
        )

    destatis_zensus_population_per_ha_inside_germany = gpd.read_postgis(
        cells_query.statement, cells_query.session.bind, index_col="id"
    )

    # add geom data of zensus cell
    missing_buildings_geom = pd.merge(
        left=destatis_zensus_population_per_ha_inside_germany[["geom"]],
        right=missing_buildings,
        left_index=True,
        right_index=True,
        how="right",
    )

    missing_buildings_geom = missing_buildings_geom.reset_index(drop=False)
    missing_buildings_geom = missing_buildings_geom.rename(
        columns={
            "building_count": "building_id",
            "cell_profile_ids": "profiles",
            "id": "cell_id",
        }
    )

    # create random points within census cells
    points = random_point_in_square(
        geom=missing_buildings_geom["geom"], tol=edge_length / 2
    )

    # Store center of poylon
    missing_buildings_geom["geom_point"] = points
    # Create building using a square around point
    missing_buildings_geom["geom_building"] = points.buffer(
        distance=edge_length / 2, cap_style=3
    )
    missing_buildings_geom = missing_buildings_geom.drop(columns=["geom"])
    missing_buildings_geom = gpd.GeoDataFrame(
        missing_buildings_geom, crs="EPSG:3035", geometry="geom_building"
    )

    # get table metadata from db by name and schema
    buildings = Table("osm_buildings", Base.metadata, schema="openstreetmap")
    inspect(engine).reflecttable(buildings, None)

    # get max number of building ids from non-filtered building table
    with db.session_scope() as session:
        buildings = session.execute(func.max(buildings.c.id)).scalar()

    # apply ids following the sequence of openstreetmap.osm_buildings id
    missing_buildings_geom["id"] = range(
        buildings + 1,
        buildings + len(missing_buildings_geom) + 1,
    )

    drop_columns = [
        i
        for i in ["building_id", "profiles"]
        if i in missing_buildings_geom.columns
    ]
    if drop_columns:
        missing_buildings_geom = missing_buildings_geom.drop(
            columns=drop_columns
        )

    missing_buildings_geom["building"] = "residential"
    missing_buildings_geom["area"] = missing_buildings_geom[
        "geom_building"
    ].area

    return missing_buildings_geom


def generate_mapping_table(
    egon_map_zensus_buildings_residential_synth,
    egon_hh_profile_in_zensus_cell,
):
    """
    Generate a mapping table for hh profiles to buildings.

    All hh demand profiles are randomly assigned to buildings within the same
    cencus cell.

    * profiles > buildings: buildings can have multiple profiles but every
        building gets at least one profile
    * profiles < buildings: not every building gets a profile


    Parameters
    ----------
    egon_map_zensus_buildings_residential_synth: pd.DataFrame
        Table with OSM and synthetic buildings ids per census cell
    egon_hh_profile_in_zensus_cell: pd.DataFrame
        Table mapping hh demand profiles to census cells

    Returns
    -------
    pd.DataFrame
        Table with mapping of profile ids to buildings with OSM ids

    """

    def create_pool(buildings, profiles):
        if profiles > buildings:
            surplus = profiles - buildings
            surplus = rng.integers(0, buildings, surplus)
            pool = list(range(buildings)) + list(surplus)
        else:
            pool = list(range(buildings))
        result = random.sample(population=pool, k=profiles)

        return result

    # group oms_ids by census cells and aggregate to list
    osm_ids_per_cell = (
        egon_map_zensus_buildings_residential_synth[["id", "cell_id"]]
        .groupby("cell_id")
        .agg(list)
    )

    # cell ids of cells with osm ids
    cells_with_buildings = osm_ids_per_cell.index.astype(int).values
    # cell ids of cells with profiles
    cells_with_profiles = (
        egon_hh_profile_in_zensus_cell["cell_id"].astype(int).values
    )
    # cell ids of cells with osm ids and profiles
    cell_with_profiles_and_buildings = np.intersect1d(
        cells_with_profiles, cells_with_buildings
    )

    # cells with only buildings might not be residential etc.

    # reduced list of profile_ids per cell with both buildings and profiles
    profile_ids_per_cell_reduced = egon_hh_profile_in_zensus_cell.set_index(
        "cell_id"
    ).loc[cell_with_profiles_and_buildings, "cell_profile_ids"]
    # reduced list of osm_ids per cell with both buildings and profiles
    osm_ids_per_cell_reduced = osm_ids_per_cell.loc[
        cell_with_profiles_and_buildings, "id"
    ].rename("building_ids")

    # concat both lists by same cell_id
    mapping_profiles_to_buildings_reduced = pd.concat(
        [profile_ids_per_cell_reduced, osm_ids_per_cell_reduced], axis=1
    )

    # count number of profiles and buildings for each cell
    # tells how many profiles have to be assigned to how many buildings
    number_profiles_and_buildings_reduced = (
        mapping_profiles_to_buildings_reduced.applymap(len)
    )

    # map profiles randomly per cell
    # if profiles > buildings, every building will get at least one profile
    rng = np.random.default_rng(RANDOM_SEED)
    random.seed(RANDOM_SEED)
    mapping_profiles_to_buildings = pd.Series(
        [
            create_pool(buildings, profiles)
            for buildings, profiles in zip(
                number_profiles_and_buildings_reduced["building_ids"].values,
                number_profiles_and_buildings_reduced[
                    "cell_profile_ids"
                ].values,
            )
        ],
        index=number_profiles_and_buildings_reduced.index,
    )

    # unnest building assignement per cell
    mapping_profiles_to_buildings = (
        mapping_profiles_to_buildings.rename("building")
        .explode()
        .reset_index()
    )
    # add profile position as attribute by number of entries per cell (*)
    mapping_profiles_to_buildings[
        "profile"
    ] = mapping_profiles_to_buildings.groupby(["cell_id"]).cumcount()
    # get multiindex of profiles in cells (*)
    index_profiles = mapping_profiles_to_buildings.set_index(
        ["cell_id", "profile"]
    ).index
    # get multiindex of buildings in cells (*)
    index_buildings = mapping_profiles_to_buildings.set_index(
        ["cell_id", "building"]
    ).index

    # get list of profiles by cell and profile position
    profile_ids_per_cell_reduced = (
        profile_ids_per_cell_reduced.explode().reset_index()
    )
    # assign profile position by order of list
    profile_ids_per_cell_reduced[
        "profile"
    ] = profile_ids_per_cell_reduced.groupby(["cell_id"]).cumcount()
    profile_ids_per_cell_reduced = profile_ids_per_cell_reduced.set_index(
        ["cell_id", "profile"]
    )

    # get list of building by cell and building number
    osm_ids_per_cell_reduced = osm_ids_per_cell_reduced.explode().reset_index()
    # assign building number by order of list
    osm_ids_per_cell_reduced["building"] = osm_ids_per_cell_reduced.groupby(
        ["cell_id"]
    ).cumcount()
    osm_ids_per_cell_reduced = osm_ids_per_cell_reduced.set_index(
        ["cell_id", "building"]
    )

    # map profiles and buildings by profile position and building number
    # merge is possible as both index results from the same origin (*) and are
    # not rearranged, therefore in the same order
    mapping_profiles_to_buildings = pd.merge(
        osm_ids_per_cell_reduced.loc[index_buildings].reset_index(drop=False),
        profile_ids_per_cell_reduced.loc[index_profiles].reset_index(
            drop=True
        ),
        left_index=True,
        right_index=True,
    )

    # rename columns
    mapping_profiles_to_buildings.rename(
        columns={
            "building_ids": "building_id",
            "cell_profile_ids": "profile_id",
        },
        inplace=True,
    )

    return mapping_profiles_to_buildings


def reduce_synthetic_buildings(
    mapping_profiles_to_buildings, synthetic_buildings
):
    """Reduced list of synthetic buildings to amount actually used.

    Not all are used, due to randomised assignment with replacing
    Id's are adapted to continuous number sequence following
    openstreetmap.osm_buildings"""

    buildings = Table("osm_buildings", Base.metadata, schema="openstreetmap")
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(buildings, None)

    # total number of buildings
    with db.session_scope() as session:
        buildings = session.execute(func.max(buildings.c.id)).scalar()

    synth_ids_used = mapping_profiles_to_buildings.loc[
        mapping_profiles_to_buildings["building_id"] > buildings,
        "building_id",
    ].unique()

    synthetic_buildings = synthetic_buildings.loc[
        synthetic_buildings["id"].isin(synth_ids_used)
    ]
    # id_mapping = dict(
    #     list(
    #         zip(
    #             synth_ids_used,
    #             range(
    #                 buildings,
    #                 buildings
    #                 + len(synth_ids_used) + 1
    #             )
    #         )
    #     )
    # )

    # time expensive because of regex
    # mapping_profiles_to_buildings['building_id'] = mapping_profiles_to_buildings['building_id'].replace(id_mapping)
    return synthetic_buildings


def get_building_peak_loads():
    """
    Peak loads of buildings are determined.

    Timeseries for every building are accumulated, the maximum value
    determined and with the respective nuts3 factor scaled for 2035 and 2050
    scenario.

    Note
    ----------
    In test-mode 'SH' the iteration takes place by 'cell_id' to avoid
    intensive RAM usage. For whole Germany 'nuts3' are taken and
    RAM > 32GB is necessary.
    """

    with db.session_scope() as session:
        cells_query = (
            session.query(
                HouseholdElectricityProfilesOfBuildings,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.factor_2019,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050,
            )
            .filter(
                HouseholdElectricityProfilesOfBuildings.cell_id
                == HouseholdElectricityProfilesInCensusCells.cell_id
            )
            .order_by(HouseholdElectricityProfilesOfBuildings.id)
        )

        df_buildings_and_profiles = pd.read_sql(
            cells_query.statement, cells_query.session.bind, index_col="id"
        )

        # Read demand profiles from egon-data-bundle
        df_profiles = get_iee_hh_demand_profiles_raw()

        def ve(s):
            raise (ValueError(s))

        dataset = egon.data.config.settings()["egon-data"][
            "--dataset-boundary"
        ]
        iterate_over = (
            "nuts3"
            if dataset == "Everything"
            else "cell_id"
            if dataset == "Schleswig-Holstein"
            else ve(f"'{dataset}' is not a valid dataset boundary.")
        )

        df_building_peak_loads = pd.DataFrame()

        for nuts3, df in df_buildings_and_profiles.groupby(by=iterate_over):
            df_building_peak_load_nuts3 = df_profiles.loc[:, df.profile_id]

            m_index = pd.MultiIndex.from_arrays(
                [df.profile_id, df.building_id],
                names=("profile_id", "building_id"),
            )
            df_building_peak_load_nuts3.columns = m_index
            df_building_peak_load_nuts3 = df_building_peak_load_nuts3.sum(
                level="building_id", axis=1
            ).max()

            df_building_peak_load_nuts3 = pd.DataFrame(
                [
                    df_building_peak_load_nuts3 * df["factor_2019"].unique(),
                    df_building_peak_load_nuts3 * df["factor_2035"].unique(),
                    df_building_peak_load_nuts3 * df["factor_2050"].unique(),
                ],
                index=[
                    "status2019",
                    "eGon2035",
                    "eGon100RE",
                ],
            ).T

            df_building_peak_loads = pd.concat(
                [df_building_peak_loads, df_building_peak_load_nuts3], axis=0
            )

        df_building_peak_loads.reset_index(inplace=True)
        df_building_peak_loads["sector"] = "residential"

        BuildingElectricityPeakLoads.__table__.drop(
            bind=engine, checkfirst=True
        )
        BuildingElectricityPeakLoads.__table__.create(
            bind=engine, checkfirst=True
        )

        df_building_peak_loads = df_building_peak_loads.melt(
            id_vars=["building_id", "sector"],
            var_name="scenario",
            value_name="peak_load_in_w",
        )

        # Write peak loads into db
        with db.session_scope() as session:
            session.bulk_insert_mappings(
                BuildingElectricityPeakLoads,
                df_building_peak_loads.to_dict(orient="records"),
            )


def map_houseprofiles_to_buildings():
    """
    Cencus hh demand profiles are assigned to buildings via osm ids. If no OSM
    ids available, synthetic buildings are generated. A list of the generated
    buildings and supplementary data as well as the mapping table is stored
    in the db.

    Tables:
    ----------
    synthetic_buildings:
        schema: openstreetmap
        tablename: osm_buildings_synthetic

    mapping_profiles_to_buildings:
        schema: demand
        tablename: egon_household_electricity_profile_of_buildings

    Notes
    -----
    """
    #
    egon_map_zensus_buildings_residential = Table(
        "egon_map_zensus_buildings_residential",
        Base.metadata,
        schema="boundaries",
    )
    # get table metadata from db by name and schema
    inspect(engine).reflecttable(egon_map_zensus_buildings_residential, None)

    with db.session_scope() as session:
        cells_query = session.query(egon_map_zensus_buildings_residential)
    egon_map_zensus_buildings_residential = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )

    with db.session_scope() as session:
        cells_query = session.query(HouseholdElectricityProfilesInCensusCells)
    egon_hh_profile_in_zensus_cell = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )  # index_col="cell_id")

    # Match OSM and zensus data to define missing buildings
    missing_buildings = match_osm_and_zensus_data(
        egon_hh_profile_in_zensus_cell,
        egon_map_zensus_buildings_residential,
    )

    # randomly generate synthetic buildings in cell without any
    synthetic_buildings = generate_synthetic_buildings(
        missing_buildings, edge_length=5
    )

    # add synthetic buildings to df
    egon_map_zensus_buildings_residential_synth = pd.concat(
        [
            egon_map_zensus_buildings_residential,
            synthetic_buildings[["id", "cell_id"]],
        ],
        ignore_index=True,
    )

    # assign profiles to buildings
    mapping_profiles_to_buildings = generate_mapping_table(
        egon_map_zensus_buildings_residential_synth,
        egon_hh_profile_in_zensus_cell,
    )

    # reduce list to only used synthetic buildings
    synthetic_buildings = reduce_synthetic_buildings(
        mapping_profiles_to_buildings, synthetic_buildings
    )
    # TODO remove unused code
    # synthetic_buildings = synthetic_buildings.drop(columns=["grid_id"])
    synthetic_buildings["n_amenities_inside"] = 0

    OsmBuildingsSynthetic.__table__.drop(bind=engine, checkfirst=True)
    OsmBuildingsSynthetic.__table__.create(bind=engine, checkfirst=True)

    # Write new buildings incl coord into db
    synthetic_buildings.to_postgis(
        "osm_buildings_synthetic",
        con=engine,
        if_exists="append",
        schema="openstreetmap",
        dtype={
            "id": OsmBuildingsSynthetic.id.type,
            "cell_id": OsmBuildingsSynthetic.cell_id.type,
            "geom_building": OsmBuildingsSynthetic.geom_building.type,
            "geom_point": OsmBuildingsSynthetic.geom_point.type,
            "n_amenities_inside": OsmBuildingsSynthetic.n_amenities_inside.type,
            "building": OsmBuildingsSynthetic.building.type,
            "area": OsmBuildingsSynthetic.area.type,
        },
    )

    HouseholdElectricityProfilesOfBuildings.__table__.drop(
        bind=engine, checkfirst=True
    )
    HouseholdElectricityProfilesOfBuildings.__table__.create(
        bind=engine, checkfirst=True
    )

    # Write building mapping into db
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            HouseholdElectricityProfilesOfBuildings,
            mapping_profiles_to_buildings.to_dict(orient="records"),
        )


setup = partial(
    Dataset,
    name="Demand_Building_Assignment",
    version="0.0.5",
    dependencies=[],
    tasks=(map_houseprofiles_to_buildings, get_building_peak_loads),
)
