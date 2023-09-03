import pandas as pd
from sqlalchemy import create_engine

# Create connection with pgAdmin4 - Offline
engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
    echo=False)


# Read the CSV file containing the new data
points_df = pd.read_csv("./egon_etrago_line_pdf_points.csv")

# Write the DataFrame to the PostgreSQL table, appending the new data
points_df.to_sql(
    "egon_etrago_bus_new",  # Table name
    engine,
    schema="grid",  # Schema name
    if_exists="append",  # Append to existing table
    index=False  # Do not write the index as a column
)


print("operation successful")

