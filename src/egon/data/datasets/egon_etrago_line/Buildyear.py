import pandas as pd

# Read the Destination file from CSV
lines_df = pd.read_csv("/home/student/powerd-data/src/egon/data/datasets/egon_etrago_line/egon_etrago_line_pdf.V1_test.csv")


lines_df['build_year'] = lines_df["anvisierte Inbetriebnahme"].fillna('').str.split(r'[,/-]').apply(lambda x: x[1].strip() if len(x) > 1 else x[0].strip())

"""
for index1, row1 in lines_df.iterrows():
    scn_name1 = str(row1['scn_name'])
    for index2, row2 in buildyear_df.iterrows():
        scn_name2 = str(row2['scn_name'])
        if scn_name1 == scn_name2:
            lines_df.loc[index1, 'build_year'] = row2['anvisierte Inbetriebnahme']
"""
# Save the updated file
lines_df.to_csv('/home/student/powerd-data/src/egon/data/datasets/egon_etrago_line/egon_etrago_line_pdf.V1_test.csv', index=False)



print("Operation successful")