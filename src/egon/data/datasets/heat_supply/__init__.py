"""The central module containing all code dealing with heat supply data

"""

import datetime
import json
import time

from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.district_heating_areas import EgonDistrictHeatingAreas
from egon.data.datasets.heat_supply.district_heating import (
    backup_gas_boilers,
    backup_resistive_heaters,
    cascade_heat_supply,
)
from egon.data.datasets.heat_supply.geothermal import potential_germany
from egon.data.datasets.heat_supply.individual_heating import (
    cascade_heat_supply_indiv,
)
from egon.data.metadata import (
    context,
    generate_resource_fields_from_sqla_model,
    license_ccby,
    license_egon_data_odbl,
    meta_metadata,
    sources,
)

# Will later be imported from another file.
Base = declarative_base()


class EgonDistrictHeatingSupply(Base):
    __tablename__ = "egon_district_heating"
    __table_args__ = {"schema": "supply"}
    index = Column(Integer, primary_key=True, autoincrement=True)
    district_heating_id = Column(
        Integer, ForeignKey(EgonDistrictHeatingAreas.id)
    )
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry("POINT", 3035))
    scenario = Column(String(50))


class EgonIndividualHeatingSupply(Base):
    __tablename__ = "egon_individual_heating"
    __table_args__ = {"schema": "supply"}
    index = Column(Integer, primary_key=True, autoincrement=True)
    mv_grid_id = Column(Integer)
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry("POINT", 3035))
    scenario = Column(String(50))


def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    engine = db.engine()
    EgonDistrictHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonDistrictHeatingSupply.__table__.create(bind=engine, checkfirst=True)
    EgonIndividualHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonIndividualHeatingSupply.__table__.create(bind=engine, checkfirst=True)


def district_heating():
    """Insert supply for district heating areas

    Returns
    -------
    None.

    """
    sources = config.datasets()["heat_supply"]["sources"]
    targets = config.datasets()["heat_supply"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['district_heating_supply']['schema']}.
        {targets['district_heating_supply']['table']}
        """
    )

    supply_2035 = cascade_heat_supply("eGon2035", plotting=False)

    supply_2035["scenario"] = "eGon2035"

    supply_2035.to_postgis(
        targets["district_heating_supply"]["table"],
        schema=targets["district_heating_supply"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    # Compare target value with sum of distributed heat supply
    df_check = db.select_dataframe(
        f"""
        SELECT a.carrier,
        (SUM(a.capacity) - b.capacity) / SUM(a.capacity) as deviation
        FROM {targets['district_heating_supply']['schema']}.
        {targets['district_heating_supply']['table']} a,
        {sources['scenario_capacities']['schema']}.
        {sources['scenario_capacities']['table']} b
        WHERE a.scenario = 'eGon2035'
        AND b.scenario_name = 'eGon2035'
        AND b.carrier = CONCAT('urban_central_', a.carrier)
        GROUP BY (a.carrier,  b.capacity);
        """
    )
    # If the deviation is > 1%, throw an error
    assert (
        df_check.deviation.abs().max() < 1
    ), f"""Unexpected deviation between target value and distributed
        heat supply: {df_check}
        """

    # Add gas boilers as conventional backup capacities
    backup = backup_gas_boilers("eGon2035")

    backup.to_postgis(
        targets["district_heating_supply"]["table"],
        schema=targets["district_heating_supply"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    backup_rh = backup_resistive_heaters("eGon2035")

    if not backup_rh.empty:
        backup_rh.to_postgis(
            targets["district_heating_supply"]["table"],
            schema=targets["district_heating_supply"]["schema"],
            con=db.engine(),
            if_exists="append",
        )


def individual_heating():
    """Insert supply for individual heating

    Returns
    -------
    None.

    """
    targets = config.datasets()["heat_supply"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['individual_heating_supply']['schema']}.
        {targets['individual_heating_supply']['table']}
        """
    )

    supply_2035 = cascade_heat_supply_indiv(
        "eGon2035", distribution_level="federal_states", plotting=False
    )

    supply_2035["scenario"] = "eGon2035"

    supply_2035.to_postgis(
        targets["individual_heating_supply"]["table"],
        schema=targets["individual_heating_supply"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def metadata():
    """Write metadata for heat supply tables

    Returns
    -------
    None.

    """

    fields = generate_resource_fields_from_sqla_model(
        EgonDistrictHeatingSupply
    )

    fields_df = pd.DataFrame(data=fields).set_index("name")
    fields_df.loc["index", "description"] = "Unique identifyer"
    fields_df.loc[
        "district_heating_id", "description"
    ] = "Index of the corresponding district heating grid"
    fields_df.loc["carrier", "description"] = "Name of energy carrier"
    fields_df.loc[
        "category", "description"
    ] = "Size-category of district heating grid"
    fields_df.loc["capacity", "description"] = "Installed heating capacity"
    fields_df.loc[
        "geometry", "description"
    ] = "Location of thermal power plant"
    fields_df.loc["scenario", "description"] = "Name of corresponing scenario"

    fields_df.loc["capacity", "unit"] = "MW_th"
    fields_df.unit.fillna("none", inplace=True)

    fields = fields_df.reset_index().to_dict(orient="records")

    meta_district = {
        "name": "supply.egon_district_heating",
        "title": "eGon heat supply for district heating grids",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Heat supply technologies for district heating grids",
        "language": ["EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": [
            sources()["era5"],
            sources()["vg250"],
            sources()["egon-data"],
            sources()["egon-data_bundle"],
            sources()["openstreetmap"],
            sources()["mastr"],
            sources()["peta"],
        ],
        "licenses": [license_egon_data_odbl()],
        "contributors": [
            {
                "title": "Clara Büttner",
                "email": "http://github.com/ClaraBuettner",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "supply.egon_district_heating",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": fields,
                    "primaryKey": ["index"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    # Add metadata as a comment to the table
    db.submit_comment(
        "'" + json.dumps(meta_district) + "'",
        EgonDistrictHeatingSupply.__table__.schema,
        EgonDistrictHeatingSupply.__table__.name,
    )

    fields = generate_resource_fields_from_sqla_model(
        EgonIndividualHeatingSupply
    )

    fields_df = pd.DataFrame(data=fields).set_index("name")
    fields_df.loc["index", "description"] = "Unique identifyer"
    fields_df.loc[
        "mv_grid_id", "description"
    ] = "Index of the corresponding mv grid district"
    fields_df.loc["carrier", "description"] = "Name of energy carrier"
    fields_df.loc["category", "description"] = "Size-category"
    fields_df.loc["capacity", "description"] = "Installed heating capacity"
    fields_df.loc[
        "geometry", "description"
    ] = "Location of thermal power plant"
    fields_df.loc["scenario", "description"] = "Name of corresponing scenario"

    fields_df.loc["capacity", "unit"] = "MW_th"
    fields_df.unit.fillna("none", inplace=True)

    fields = fields_df.reset_index().to_dict(orient="records")

    meta_district = {
        "name": "supply.egon_individual_heating",
        "title": "eGon heat supply for individual supplied buildings",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Heat supply technologies for individual supplied buildings",
        "language": ["EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": [
            sources()["era5"],
            sources()["vg250"],
            sources()["egon-data"],
            sources()["egon-data_bundle"],
            sources()["openstreetmap"],
            sources()["mastr"],
            sources()["peta"],
        ],
        "licenses": [license_egon_data_odbl()],
        "contributors": [
            {
                "title": "Clara Büttner",
                "email": "http://github.com/ClaraBuettner",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "supply.egon_individual_heating",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": fields,
                    "primaryKey": ["index"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    # Add metadata as a comment to the table
    db.submit_comment(
        "'" + json.dumps(meta_district) + "'",
        EgonIndividualHeatingSupply.__table__.schema,
        EgonIndividualHeatingSupply.__table__.name,
    )


class HeatSupply(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatSupply",
            version="0.0.9",
            dependencies=dependencies,
            tasks=(
                create_tables,
                {
                    district_heating,
                    individual_heating,
                    potential_germany,
                },
                metadata,
            ),
        )
