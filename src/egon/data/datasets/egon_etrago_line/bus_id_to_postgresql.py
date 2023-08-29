import shapely.wkb as wkb
from shapely.geometry import Point
from shapely.geometry import LineString
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine


# Create connection with pgAdmin4 - Offline
engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
    echo=False)

#original_table from pgAdmin
substation_df = pd.read_sql(
    """
    SELECT * FROM grid.egon_etrago_bus
  
    """
    , engine)


unique_bus_id = substation_df['bus_id'].max()

# new coordinates for point Kreuzung M24b/TTG-006 
coordinates_1="10.297685 52.207571"
lon_1, lat_1 = map(float, coordinates_1.split())
point_1 = Point(lon_1, lat_1)

# new coordinates for point Schraplau/Obhausen
coordinates_2="11.658435 51.420564"
lon_2, lat_2 = map(float, coordinates_1.split())
point_2 = Point(lon_2, lat_2)

# new coordinates for point sanitz dettmannsdorf
coordinates_3 ="12.456474 54.075519"
lon_3, lat_3 = map(float, coordinates_1.split())
point_3 = Point(lon_3, lat_3)

# converting points into WKB-HEX-Format
wkb_hex_1 = wkb.dumps(point_1, hex=True)
wkb_hex_2 = wkb.dumps(point_2, hex=True)
wkb_hex_3 = wkb.dumps(point_3, hex=True)

# checking if format is right
print(wkb_hex_1, wkb_hex_2, wkb_hex_3)
