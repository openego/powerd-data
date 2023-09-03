import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from geopy.distance import geodesic
import difflib
from shapely.geometry import Point
from shapely.geometry import LineString


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
lines_df = pd.read_csv("./egon_etrago_line_pdf.csv")

formatted_point_0 = None
formatted_point_1 = None

for index, row in lines_df.iterrows():

    formatted_point_0 = None
    formatted_point_1 = None 
    
    bus_0 = row['bus0']
    bus_1 = row['bus1']
    
    # Match Similarity of Source & Destination files for Start point  
    matching_rows_start = substation_df[substation_df['bus_id'] == lines_df.at[index, 'bus0']]

    if not matching_rows_start.empty:
        
        # Find coordinate for start point
        if pd.isnull(lines_df.at[index, 'Coordinate0']):
            point_0 = matching_rows_start.iloc[0]['geom']
            formatted_point_0 = f"{point_0.x} {point_0.y}"
            lines_df.at[index, 'Coordinate0'] = formatted_point_0 
        
    # Match Similarity of Source & Destination files for End point                           
    matching_rows_end = substation_df[substation_df['bus_id'] == lines_df.at[index, 'bus1']]
    if not matching_rows_end.empty:
        
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
                lines_df.at[index, 'length'] = round(distance*1.14890133371257,1)
                lines_df.at[index, 'length1'] = f'B {round(distance*1.14890133371257,1)}'
                


    #Filling empty cells
    if pd.isnull (lines_df.at[index, 's_nom']):
        if (lines_df.at[index, 'cable/line'] == 'line'):
            lines_df.at[index,'s_nom'] = 1790
        if (lines_df.at[index,'cable/line'] == 'cable'):
            lines_df.at[index,'s_nom'] = 925
    if pd.isnull (lines_df.at[index, 'cables']):
        lines_df.at[index, 'cables'] = 3
        lines_df.at[index, 'num_parallel'] = 1
    lines_df.at[index, 's_nom_min'] = lines_df.at[index, 's_nom']
    lines_df.at[index, 's_nom_max'] = 'Infinity'
    lines_df.at[index, 's_nom_extendable'] = 'true'
    lines_df.at[index, 'v_ang_max'] = 'Infinity'
    lines_df.at[index, 'v_ang_min'] = '-Infinity'
    lines_df.at[index, 'terrain_factor'] = 1

    #calculation of X, R, Cost
    lines_df['length'] = pd.to_numeric(lines_df['length'], errors='coerce')
    if (lines_df.at[index,'v_nom'] == 380) and (lines_df.at[index, 'cable/line'] == 'line'):
        lines_df.at[index,'r'] = 0.028 / (lines_df.at[index,'s_nom']/1790) * lines_df.at[index, 'length']
        lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.8 / (lines_df.at[index,'s_nom']/1790) * lines_df.at[index, 'length']
        lines_df.at[index,'capital_cost'] = 2500000 / lines_df.at[index,'s_nom']*lines_df.at[index, 'length']/(lines_df.at[index, 'cables']/3)

    if (lines_df.at[index,'v_nom'] == 380) and (lines_df.at[index, 'cable/line'] == 'cable'):
        lines_df.at[index,'r'] = 0.0175 / (lines_df.at[index,'s_nom']/925) * lines_df.at[index, 'length']
        lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.3 / (lines_df.at[index,'s_nom']/925) * lines_df.at[index, 'length']
        lines_df.at[index,'capital_cost'] = 11500000 / lines_df.at[index,'s_nom']*lines_df.at[index, 'length']/(lines_df.at[index, 'cables']/3)

    if (lines_df.at[index,'carrier'] == 'DC') and (lines_df.at[index, 'cable/line'] == 'line'):
        lines_df.at[index,'r'] = 0.0175 / (lines_df.at[index,'s_nom']/925) * lines_df.at[index, 'length']
        lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.3 / (lines_df.at[index,'s_nom']/925) * lines_df.at[index, 'length']
        lines_df.at[index,'capital_cost'] = 500000 / lines_df.at[index,'s_nom']*lines_df.at[index, 'length']/(lines_df.at[index, 'cables']/3)

    if (lines_df.at[index,'carrier'] == 'DC') and (lines_df.at[index, 'cable/line'] == 'cable'):
        lines_df.at[index,'r'] = 0.0175 / (lines_df.at[index,'s_nom']/925) * lines_df.at[index, 'length']
        lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.3 / (lines_df.at[index,'s_nom']/925) * lines_df.at[index, 'length']
        lines_df.at[index,'capital_cost'] = 3250000 / lines_df.at[index,'s_nom']*lines_df.at[index, 'length']/(lines_df.at[index, 'cables']/3)

     # Converting Coordinates into wbk_hex-format
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
                    
# Save the updated file
lines_df.to_csv('./egon_etrago_line_pdf.csv', index=False)
print("Operation successful")