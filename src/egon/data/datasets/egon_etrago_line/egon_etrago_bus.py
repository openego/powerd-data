import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from geopy.distance import geodesic
import difflib

# Create connection with pgAdmin4 - Offline
engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
    echo=False,)

# Read the Source files
substation_df = pd.read_sql(
    """
    SELECT * FROM grid.egon_etrago_bus
  
    """
    , engine)


substation_df = gpd.read_postgis(
    """
    SELECT * FROM grid.egon_etrago_bus
    
    """
    , engine, geom_col="geom")


existing_lines_df = pd.read_sql(
    """
    SELECT line_id FROM grid.egon_etrago_line  
    """
    , engine)


# Read the Destination file from CSV
lines_df = pd.read_csv("./egon_etrago_line_pdf_test.csv")

formatted_point_0 = None
formatted_point_1 = None

for index, row in lines_df.iterrows():

    formatted_point_0 = None
    formatted_point_1 = None 
    
    bus_0 = row['bus0']
    bus_1 = row['bus1']
    
    # Match Similarity of Source & Destination files for Start point  
    matching_rows_start = substation_df[substation_df['bus_id'] == bus_0]

    if not matching_rows_start.empty:
        
        # if pd.isnull(lines_df.at[index, 'bus0']):
        #     lines_df.at[index, 'bus0'] = matching_rows_start.iloc[0]['bus_id']
        # if pd.isnull(lines_df.at[index, 'subst_name0']):
        #     lines_df.at[index, 'subst_name0'] = {matching_rows_start.iloc[0]['subst_name']}
        
        # Find coordinate for start point
        if pd.isnull(lines_df.at[index, 'Coordinate0']):
            point_0 = matching_rows_start.iloc[0]['geom']
            formatted_point_0 = f"{point_0.x} {point_0.y}"
            lines_df.at[index, 'Coordinate0'] = formatted_point_0 
        
    # Match Similarity of Source & Destination files for End point                           
    matching_rows_end = substation_df[substation_df['bus_id'] == bus_1gitt]
    if not matching_rows_end.empty:
        
        # if pd.isnull(lines_df.at[index, 'bus1']):
        #     lines_df.at[index, 'bus1'] = matching_rows_end.iloc[0]['bus_id']
        # if pd.isnull(lines_df.at[index, 'subst_name1']):
        #     lines_df.at[index, 'subst_name1'] = matching_rows_end.iloc[0]['subst_name']

        # Find coordinate for End point
        if pd.isnull(lines_df.at[index, 'Coordinate1']):
            point_1 = matching_rows_end.iloc[0]['geom']
            formatted_point_1 = f"{point_1.x} {point_1.y}"
            lines_df.at[index, 'Coordinate1'] = formatted_point_1


        # Calculate lenght of Transmission Line
        if pd.notna(lines_df.at[index, 'Coordinate0']) and pd.notna(lines_df.at[index, 'Coordinate1']):
            if pd.isnull(lines_df.at[index, 'length']):
                coordinate_0_str = str(lines_df.at[index, 'Coordinate0'])
                lon0, lat0 = map(float, coordinate_0_str.split(' '))
                coordinate_1_str = str(lines_df.at[index, 'Coordinate1'])
                lon1, lat1 = map(float, coordinate_1_str.split(' '))
                distance = geodesic((lat0, lon0), (lat1, lon1)).kilometers
                lines_df.at[index, 'length'] = f'TB {round(distance*1.14890133371257,1)}'

# Save the updated file
lines_df.to_csv('./egon_etrago_line_pdf_test.csv', index=False)
print("Operation successful")