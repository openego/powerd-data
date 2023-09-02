import math
import pandas as pd


# Read the Destination file from CSV
lines_df = pd.read_csv("./NEP_tables_6August2023 - test.csv")

L = 0.8   #mH/km   value from paramters.py for s_nom = 1790, 380kV
R = 0.0175  #Ohm/km     value from paramters.py for s_nom = 1790, 380kV
s_nom = 1790   #MVA     standart value from paramters.py
AC_cost_factor = 2.5e6    #for ac_line_380kV from parameters.py
DC_cost_factor = 0.5e3    #for dc_overhead_line from parameters.py

for index, row in lines_df.iterrows():
    
    lines_df.at[index,'x'] = 2*math.pi*50*L*0.001*lines_df.at[index,'length']
    lines_df.at[index,'r'] = R*lines_df.at[index,'length']
    
    if pd.isnull(lines_df.at[index,'s_nom']):
        if pd.isnull(lines_df.at[index,'num_parallel']):
            lines_df.at[index,'s_nom'] = s_nom
            lines_df.at[index,'cables'] = f'A {3}'
            lines_df.at[index,'num_parallel'] = f'A {1}'
        else: 
            lines_df.at[index,'s_nom'] = s_nom*lines_df.at[index,'num_parallel']
    
    lines_df.at[index,'s_nom_extendable'] = True
    lines_df.at[index,'s_nom_min'] = lines_df.at[index,'s_nom']
    lines_df.at[index,'s_nom_max'] = math.inf
    lines_df.at[index,'v_ang_min'] = -math.inf
    lines_df.at[index,'v_ang_max'] = math.inf
    lines_df.at[index,'terrain_factor'] = 1
    """
    if lines_df[index,'carrier'] == 'AC':
        lines_df[index,'capital_cost'] = AC_cost_factor*lines_df[index,'length']
    if lines_df[index,'carrier'] == 'DC':
        lines_df[index,'capital_cost'] = DC_cost_factor*lines_df[index,'length']
    """
lines_df.to_csv('./NEP_tables_6August2023 - test.csv', index=False)

print("operation successful")



            


