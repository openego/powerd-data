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

# Read the Destination file from CSV
lines_df = pd.read_csv("./egon_etrago_line_pdf_test.csv")

for index, row in lines_df.iterrows():

    # convert coordinates into geom/topo-column
    if pd.notna(lines_df.at[index, 'Coordinate0']) and pd.notna(lines_df.at[index, 'Coordinate1']):
        coordinates_1 = str(lines_df.at[index, 'Coordinate0'])
        coordinates_2 = str(lines_df.at[index, 'Coordinate1'])
        lon_1, lat_1 = map(float, coordinates_1.split(" "))
        lon_2, lat_2 = map(float, coordinates_2.split(" "))
        point_1 = Point(lon_1, lat_1)
        point_2 = Point(lon_2, lat_2)
        geom = LineString([(lon_1, lat_1), (lon_2, lat_2)])
        wkb_hex = geom.wkb_hex
        lines_df.at[index, 'geom'] = wkb_hex
        lines_df.at[index, 'topo'] = wkb_hex

        
lines_df.to_csv('./egon_etrago_line_pdf_test.csv', index=False)
print("Operation successful")
    

