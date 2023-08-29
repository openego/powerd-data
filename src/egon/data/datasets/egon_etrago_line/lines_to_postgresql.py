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


existing_lines_df = pd.read_sql(
    """
    SELECT * FROM grid.egon_etrago_line  
    """
    , engine)


# Read the Destination file from CSV
lines_df = pd.read_csv("./egon_etrago_line_pdf_test.csv")

#columns for merge
selected_columns = ['scn_name',	'line_id',	'bus0',	'bus1',	'type',	'carrier',	'x', 'r',	'g',	'b',	's_nom',	's_nom_extendable',	's_nom_min',	's_nom_max',	's_max_pu',	'build_year',	'lifetime',	'capital_cost',	'length',	'cables',	'terrain_factor',	'num_parallel',	'v_ang_min',	'v_ang_max',	'v_nom',	'geom',	'topo']

all_lines_df = pd.concat([existing_lines_df[selected_columns], lines_df[selected_columns]], ignore_index=True)

all_lines_df.to_csv('./egon_etrago_all_lines.csv', index=False)

all_lines_df.to_sql('egon_etrago_line_NEP2035', engine, schema='grid', if_exists='replace', index=False)

print("operation successful")

