"""
Central module containing all code dealing with importing era5 weather data.
"""

import atlite
import os
from pathlib import Path

import geopandas as gpd
import egon.data.config
from egon.data import db
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.datasets import Dataset
from sqlalchemy import Column, String, Float, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

# will be later imported from another file ###
Base = declarative_base()


class WeatherData(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Era5",
            version="0.0.2",
            dependencies=dependencies,
            tasks=({create_tables, download_era5}, insert_weather_cells),
        )


class EgonEra5Cells(Base):
    __tablename__ = "egon_era5_weather_cells"
    __table_args__ = {"schema": "supply"}
    w_id = Column(Integer, primary_key=True)
    geom = Column(Geometry("POLYGON", 4326))
    geom_point = Column(Geometry("POINT", 4326))


class EgonRenewableFeedIn(Base):
    __tablename__ = "egon_era5_renewable_feedin"
    __table_args__ = {"schema": "supply"}
    w_id = Column(Integer, primary_key=True)
    weather_year = Column(Integer, primary_key=True)
    carrier = Column(String, primary_key=True)
    feedin = Column(ARRAY(Float()))


def create_tables():

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {EgonEra5Cells.__table__.schema}.{EgonEra5Cells.__table__.name} CASCADE;
        """
    )
    EgonEra5Cells.__table__.create(bind=engine, checkfirst=True)
    EgonRenewableFeedIn.__table__.drop(bind=engine, checkfirst=True)
    EgonRenewableFeedIn.__table__.create(bind=engine, checkfirst=True)


def import_cutout(boundary="Europe"):
    """Import weather data from cutout

    Returns
    -------
    cutout : atlite.cutout.Cutout
        Weather data stored in cutout

    """
    # Scenario status_quo
    try:
        status_names = egon.data.config.settings()["egon-data"]["--scenarios"]
    except Exception:
        status_names = ["status2019"]

    for status_name in status_names:
        print("for import_cutout, weather_year = get_sector_parameters, "
              f"using scenario: {status_name} of total {status_names}")
        break

    weather_year = get_sector_parameters("global", status_name)["weather_year"]

    if boundary == "Europe":
        xs = slice(-12.0, 35.1)
        ys = slice(72.0, 33.0)

    elif boundary == "Germany":
        geom_de = (
            gpd.read_postgis(
                "SELECT geometry as geom FROM boundaries.vg250_sta_bbox",
                db.engine(),
            )
            .to_crs(4326)
            .geom
        )
        xs = slice(geom_de.bounds.minx[0], geom_de.bounds.maxx[0])
        ys = slice(geom_de.bounds.miny[0], geom_de.bounds.maxy[0])

    elif boundary == "Germany-offshore":
        xs = slice(5.5, 14.5)
        ys = slice(55.5, 53.5)

    else:
        print(
            f"Boundary {boundary} not defined. "
            "Choose either 'Europe' or 'Germany'"
        )

    directory = (
        Path(".")
        / (
            egon.data.config.datasets()["era5_weather_data"]["targets"][
                "weather_data"
            ]["path"]
        )
        / f"{boundary.lower()}-{str(weather_year)}-era5.nc"
    )

    print(f"directory for era5 is {str(directory)}")

    print(f"trying to fetch data for weather_year {weather_year}")

    try:  # legacy
        cutout = atlite.Cutout(
            path=directory.absolute(),
            module="era5",
            x=xs,
            y=ys,
            years=slice(weather_year, weather_year),
        )
        print(f"Could run cutout = atlite.Cutout for weather_year {weather_year} with success.")
    except Exception:  # new
        cutout = atlite.Cutout(
            path=directory.absolute(),
            module="era5",
            x=xs,
            y=ys,
            time=str(weather_year),
        )
        print(f"Could run cutout = atlite.Cutout for weather_year {weather_year} with success.")

    return cutout


def download_era5():
    """Download weather data from era5

    Returns
    -------
    None.

    """

    directory = Path(".") / (
        egon.data.config.datasets()["era5_weather_data"]["targets"][
            "weather_data"
        ]["path"]
    )

    print("0")
    if not os.path.exists(directory):

        os.mkdir(directory)

    print("1, empty")
    cutout = import_cutout()

    if not cutout.prepared:

        cutout.prepare()

    print("2, Germany")
    cutout = import_cutout("Germany")

    if not cutout.prepared:

        cutout.prepare()

    print("3, Germany-offshore")
    cutout = import_cutout("Germany-offshore")

    if not cutout.prepared:

        cutout.prepare()


def insert_weather_cells():
    """Insert weather cells from era5 into database table

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["era5_weather_data"]

    db.execute_sql(
        f"""
        DELETE FROM {cfg['targets']['weather_cells']['schema']}.
        {cfg['targets']['weather_cells']['table']}
        """
    )

    cutout = import_cutout()

    df = gpd.GeoDataFrame(
        {"geom": cutout.grid.geometry}, geometry="geom", crs=4326
    )

    df.to_postgis(
        cfg["targets"]["weather_cells"]["table"],
        schema=cfg["targets"]["weather_cells"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    db.execute_sql(
        f"""UPDATE {cfg['targets']['weather_cells']['schema']}.
        {cfg['targets']['weather_cells']['table']}
        SET geom_point=ST_Centroid(geom);"""
    )
