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
    echo=False)

# Read the Source files
substation_df = pd.read_sql(
    """
    SELECT * FROM grid.egon_ehv_substation
  
    """
    , engine)


substation_df = gpd.read_postgis(
    """
    SELECT * FROM grid.egon_ehv_substation
    
    """
    , engine, geom_col="point")


existing_lines_df = pd.read_sql(
    """
    SELECT line_id FROM grid.egon_etrago_line  
    """
    , engine)


# Read the Destination file from CSV
lines_df = pd.read_csv("./egon_etrago_line_pdf_test.csv")

unique_line_id = existing_lines_df['line_id'].max()
formatted_point_0 = None
formatted_point_1 = None

for index, row in lines_df.iterrows():
    # Add Unique line id
    unique_line_id += 1
    lines_df.at[index, 'line_id'] = unique_line_id

    Startpunkt = str(row['Startpunkt'])
    Endpunkt = str(row['Endpunkt'])
    
    # Match Similarity of Source & Destination files for Start point  
    matching_rows_start = substation_df[substation_df['subst_name'].str.contains(Startpunkt, case=False, na=False, regex=False)]
    if not matching_rows_start.empty:
        
        if pd.isnull(lines_df.at[index, 'bus0']):
            lines_df.at[index, 'bus0'] = matching_rows_start.iloc[0]['bus_id']
        if pd.isnull(lines_df.at[index, 'subst_name0']):
            lines_df.at[index, 'subst_name0'] = matching_rows_start.iloc[0]['subst_name']

        # Find coordinate for start point
        if pd.isnull(lines_df.at[index, 'Coordinate0']):
            point_0 = matching_rows_start.iloc[0]['point']
            formatted_point_0 = f"{point_0.x} {point_0.y}"
            lines_df.at[index, 'Coordinate0'] = formatted_point_0    

        # Calculate the matching percentage
        matching_percentage_start = difflib.SequenceMatcher(None, Startpunkt, matching_rows_start.iloc[0]['subst_name']).ratio() * 100
        lines_df.at[index, 'matching0%'] = round(matching_percentage_start,0)

    # Match Similarity of Source & Destination files for End point                           
    matching_rows_end = substation_df[substation_df['subst_name'].str.contains(Endpunkt, case=False, na=False, regex=False)]
    if not matching_rows_end.empty:
        
        if pd.isnull(lines_df.at[index, 'bus1']):
            lines_df.at[index, 'bus1'] = matching_rows_end.iloc[0]['bus_id']
        if pd.isnull(lines_df.at[index, 'subst_name1']):
            lines_df.at[index, 'subst_name1'] = matching_rows_end.iloc[0]['subst_name']

        # Find coordinate for End point
        if pd.isnull(lines_df.at[index, 'Coordinate1']):
            point_1 = matching_rows_end.iloc[0]['point']
            formatted_point_1 = f"{point_1.x} {point_1.y}"
            lines_df.at[index, 'Coordinate1'] = formatted_point_1

        # Calculate the matching percentage
        matching_percentage_end = difflib.SequenceMatcher(None, Endpunkt, matching_rows_end.iloc[0]['subst_name']).ratio() * 100
        lines_df.at[index, 'matching1%'] = round(matching_percentage_end,0)

        # Calculate lenght of Transmission Line
        if pd.notna(lines_df.at[index, 'Coordinate0']) and pd.notna(lines_df.at[index, 'Coordinate1']):
            if pd.isnull(lines_df.at[index, 'length']):
                coordinate_0_str = str(lines_df.at[index, 'Coordinate0'])
                lon0, lat0 = map(float, coordinate_0_str.split(' '))
                coordinate_1_str = str(lines_df.at[index, 'Coordinate1'])
                lon1, lat1 = map(float, coordinate_1_str.split(' '))
                distance = geodesic((lat0, lon0), (lat1, lon1)).kilometers
                lines_df.at[index, 'length'] = f'HV {round(distance*1.14890133371257,1)}'

    # #calculation of X, R, Cost
    # lines_df['length'] = pd.to_numeric(lines_df['length'], errors='coerce')
    # if (lines_df.at[index,'v_nom'] == 380) and (lines_df.at[index, 'cable/line'] == 'line'):
    #     lines_df.at[index,'r'] = 0.028 / (lines_df.at[index,'s_nom']/1790 * lines_df.at[index, 'length'])
    #     lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.8 / (lines_df.at[index,'s_nom']/1790 * lines_df.at[index, 'length'])
    #     lines_df.at[index,'capital_cost'] = 2500000 * lines_df.at[index, 'length']

    # if (lines_df.at[index,'v_nom'] == 380) and (lines_df.at[index, 'cable/line'] == 'cable'):
    #     lines_df.at[index,'r'] = 0.0175 / (lines_df.at[index,'s_nom']/925 * lines_df.at[index, 'length'])
    #     lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.3 / (lines_df.at[index,'s_nom']/925 * lines_df.at[index, 'length'])
    #     lines_df.at[index,'capital_cost'] = 11500000 * lines_df.at[index, 'length']

    # if (lines_df.at[index,'carrier'] == 'DC') and (lines_df.at[index, 'cable/line'] == 'line'):
    #     lines_df.at[index,'r'] = 0.0175 / (lines_df.at[index,'s_nom']/925 * lines_df.at[index, 'length'])
    #     lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.3 / (lines_df.at[index,'s_nom']/925 * lines_df.at[index, 'length'])
    # if (lines_df.at[index,'carrier'] == 'DC') and (lines_df.at[index, 'cable/line'] == 'cable'):
    #     lines_df.at[index,'r'] = 0.0175 / (lines_df.at[index,'s_nom']/925 * lines_df.at[index, 'length'])
    #     lines_df.at[index,'x'] = 2*3.14159*50*0.001*0.3 / (lines_df.at[index,'s_nom']/925 * lines_df.at[index, 'length'])
    
    
    # # Find similar existing line according to the location
    # if (existing_lines_df['bus0'] == lines_df.at[index, 'bus0']) and (existing_lines_df['bus1'] == lines_df.at[index, 'bus1']):
    #     lines_df.at[index, 'oldlinecode'] = existing_lines_df['line_id']
       
        
lines_df.to_csv('./egon_etrago_line_pdf_test.csv', index=False)
print("Operation successful")