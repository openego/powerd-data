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
    echo=False,
)

# Read the Source file
substation_df = pd.read_sql(
    """

    SELECT * FROM grid.egon_hvmv_substation;
    
    """
    , engine)



substation_df = gpd.read_postgis(
    """
  
    SELECT * FROM grid.egon_hvmv_substation;
    
    """
    , engine, geom_col="point")



# Read the Destination file from CSV
lines_df = pd.read_csv("./NEP_tables_V2 - first table25July2023_test.csv")

existing_lines_df = pd.read_sql(
    """
    SELECT line_id FROM grid.egon_etrago_line   
    """
    , engine)


# Match Similarity of Source & Destination files 
unique_line_id = existing_lines_df['line_id'].max()

formatted_point_0 = None
formatted_point_1 = None

for index, row in lines_df.iterrows():
    # Add Unique line id
    unique_line_id += 1
    lines_df.at[index, 'line_id'] = unique_line_id
    
    Startpunkt = str(row['Startpunkt'])
    Endpunkt = str(row['Endpunkt'])
    
    # Only when bus_id is not filled up yet
    if pd.isna(row['bus0']):   
        #matching_rows_start = substation_df[substation_df['subst_name'].apply(lambda x: any(difflib.SequenceMatcher(None, word, Startpunkt).ratio() >= 0.9 for word in x.split()))]
        matching_rows_start = substation_df[substation_df['subst_name'].str.contains(Startpunkt, case=False, na=False, regex=False)]
       
        if not matching_rows_start.empty:
            lines_df.at[index, 'bus0'] = matching_rows_start.iloc[0]['bus_id']
            lines_df.at[index, 'subst_name0'] = matching_rows_start.iloc[0]['subst_name']

            # Find coordinate for start point
            point_0 = matching_rows_start.iloc[0]['point']
            formatted_point_0 = f"{point_0.x} {point_0.y}"
            lines_df.at[index, 'Coordinate0'] = formatted_point_0

            """
            # Calculate the matching percentage
            matching_percentage_start = difflib.SequenceMatcher(None, Startpunkt, matching_rows_start.iloc[0]['subst_name']).ratio() * 100
            lines_df.at[index, 'matching1%'] = round(matching_percentage_start,2) 
                    
            # if best_match_end is None:
            #     matching_rows_end = substation_df[substation_df['subst_name'].apply(
            #         lambda x: any(difflib.SequenceMatcher(None, word, Endpunkt).ratio() >= r for word in x.split()))]
            """
    
    if pd.isna(row['bus1']):
        #matching_rows_end = substation_df[substation_df['subst_name'].apply(lambda x: any(difflib.SequenceMatcher(None, word, Endpunkt).ratio() >= 0.9   for word in x.split()))]
        matching_rows_end = substation_df[substation_df['subst_name'].str.contains(Endpunkt, case=False, na=False, regex=False)]
        
        if not matching_rows_end.empty:
            best_match_end = 1
            lines_df.at[index, 'bus1'] = matching_rows_end.iloc[0]['bus_id']
            lines_df.at[index, 'subst_name1'] = matching_rows_end.iloc[0]['subst_name']

            # Find coordinate for end point
            point_1 = matching_rows_end.iloc[0]['point']
            formatted_point_1 = f"{point_1.x} {point_1.y}"
            lines_df.at[index, 'Coordinate1'] = formatted_point_1

            """
            # Calculate the matching percentage
            matching_percentage_end = difflib.SequenceMatcher(None, Endpunkt, matching_rows_end.iloc[0]['subst_name']).ratio() * 100
            lines_df.at[index, 'matching2%'] = round(matching_percentage_end,2)
            """

            """
            if pd.notna(formatted_point_0) and pd.notna(formatted_point_1):
                lon0, lat0 = map(float, formatted_point_0.split(' '))
                lon1, lat1 = map(float, formatted_point_1.split(' '))
                distance = geodesic((lat0, lon0), (lat1, lon1)).kilometers
                lines_df.at[index, 'length'] = distance
            """

# Save the updated file
lines_df.to_csv('./NEP_tables_V2 - first table25July2023_test.csv', index=False)
print("Operation successful")