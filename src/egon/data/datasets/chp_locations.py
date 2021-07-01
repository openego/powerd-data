# -*- coding: utf-8 -*-
"""
Test if the capacities of MaStR and NEP are matching to use them later as key.
"""
import pandas as pd
import geopandas
from egon.data import db, config
from egon.data.processing.power_plants import (
    EgonPowerPlants, assign_voltage_level, assign_bus_id)
from sqlalchemy.orm import sessionmaker

def map_carrier_nep_mastr():
    """Map carriers from NEP to carriers from MaStR

    Returns
    -------
    pandas.Series
        List of mapped carriers

    """
    return (
        pd.Series(data={
        'Abfall': "Sonstige_Energietraeger",
        'Erdgas': 'Erdgas',
        'Sonstige\nEnergieträger': "Sonstige_Energietraeger",
        'Steinkohle': 'Steinkohle',
        'Kuppelgase': 'Kuppelgase',
        'Mineralöl-\nprodukte': 'Mineraloelprodukte'

        }))

def map_carrier_egon_mastr():
    """Map carriers from MaStR to carriers used in egon-data

    Returns
    -------
    pandas.Series
        List of mapped carriers

    """
    return (
        pd.Series(data={
            'Steinkohle': 'coal',
            'Erdgas': 'gas',
            'Kuppelgase': 'gas',
            'Mineraloelprodukte': 'oil',
            'NichtBiogenerAbfall': 'other_non_renewable',
            'AndereGase': 'other_non_renewable',
            'Waerme': 'other_non_renewable',
            'Sonstige_Energietraeger': 'other_non_renewable',
            }))


#####################################   NEP treatment   #################################
def select_chp_from_nep():
    sources = config.datasets()["chp_location"]["sources"]

    # Select CHP plants with geolocation from list of conventional power plants
    chp_NEP_data = db.select_dataframe(
        f"""
        SELECT bnetza_id, name, carrier, chp, postcode, capacity,
           c2035_chp, c2035_capacity
        FROM {sources['list_conv_pp']['schema']}.
        {sources['list_conv_pp']['table']}
        WHERE bnetza_id != 'KW<10 MW'
        AND (chp = 'Ja' OR c2035_chp = 'Ja')
        AND c2035_capacity > 0
        AND postcode != 'None'
        """
        )

    # Removing CHP out of Germany
    chp_NEP_data['postcode'] = chp_NEP_data['postcode'].astype(str)
    chp_NEP_data = chp_NEP_data[ ~ chp_NEP_data['postcode'].str.contains('A')]
    chp_NEP_data = chp_NEP_data[ ~ chp_NEP_data['postcode'].str.contains('L')]
    chp_NEP_data = chp_NEP_data[ ~ chp_NEP_data['postcode'].str.contains('nan') ]

    # Remove the subunits from the bnetza_id
    chp_NEP_data['bnetza_id'] = chp_NEP_data['bnetza_id'].str[0:7]

    map_carrier = map_carrier_nep_mastr()

    chp_NEP_data['carrier'] = map_carrier[chp_NEP_data['carrier'].values].values

    chp_NEP = pd.DataFrame(
        columns = ['name', 'postcode', 'carrier', 'capacity',
                   'c2035_capacity', 'c2035_chp'])

    chp_NEP = chp_NEP.append(
        chp_NEP_data[chp_NEP_data.name.isnull()].loc[:, [
            'name', 'postcode', 'carrier', 'capacity',
            'c2035_capacity',  'c2035_chp']])

    chp_NEP = chp_NEP.append(
        chp_NEP_data.groupby(
            ['carrier', 'name', 'postcode', 'c2035_chp']
            )['capacity','c2035_capacity'].sum().reset_index()).reset_index()


    return chp_NEP.drop('index', axis=1)

#####################################   MaStR treatment   #################################
def select_chp_from_mastr():

    sources = config.datasets()["chp_location"]["sources"]

    MaStR_konv = pd.read_csv(
        sources["mastr_combustion"],
        delimiter = ',',
        usecols = ['Nettonennleistung',
                    'EinheitMastrNummer',
                    'Kraftwerksnummer',
                    'Energietraeger',
                    'Postleitzahl',
                    'Laengengrad',
                    'Breitengrad',
                    'ThermischeNutzleistung',
                    'EinheitBetriebsstatus',
                    'LokationMastrNummer'])

    MaStR_konv = MaStR_konv.rename(columns={ 'Kraftwerksnummer': 'bnetza_id',
                                      'Energietraeger': 'energietraeger_Ma',
                                      'Postleitzahl': 'plz_Ma',
                                      'Laengengrad': 'longitude',
                                      'Breitengrad': 'latitude'})

    MaStR_konv = MaStR_konv[MaStR_konv.EinheitBetriebsstatus=='InBetrieb']
    # 66931 of 68321

    # Insert geometry column
    MaStR_konv = MaStR_konv[ ~ ( MaStR_konv['longitude'].isnull()) ]
    # 15777 of 68321
    MaStR_konv = geopandas.GeoDataFrame(
        MaStR_konv, geometry=geopandas.points_from_xy(
            MaStR_konv['longitude'], MaStR_konv['latitude']))

    MaStR_konv = MaStR_konv[(MaStR_konv['Nettonennleistung'] >= 100)]

    MaStR_konv = MaStR_konv[~MaStR_konv['plz_Ma'].isnull()]
    MaStR_konv['plz_Ma'] = MaStR_konv['plz_Ma'].astype(int)

    # Calculate power in MW
    MaStR_konv.loc[:, 'Nettonennleistung'] *=1e-3
    MaStR_konv.loc[:, 'ThermischeNutzleistung'] *=1e-3

    return MaStR_konv


# ############################################   Match with plz and K   ############################################
def match_nep_chp(chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1):


    for ET in chp_NEP['carrier'].unique():

        print('**********************  ' + ET + '  **********************')
        if ET == 'Kuppelgase':
            carrier = ['Erdgas', 'AndereGase','Mineraloelprodukte' ]

        elif ET == 'Sonstige_Energietraeger':
            carrier = [
                    'Erdgas', 'AndereGase','Mineraloelprodukte',
                    'Waerme','NichtBiogenerAbfall']

        elif ET == 'Mineraloelprodukte':
            carrier = ['Erdgas', 'Mineraloelprodukte' ]
        else:
            carrier = [ET]

        carrier_egon = map_carrier_egon_mastr()

        for index, row in chp_NEP[(chp_NEP['carrier']  == ET)
                                  & (chp_NEP['postcode']  != 'None')].iterrows():
            K_NEP = row['capacity']
            plz_NEP = row['postcode']
            selected = MaStR_konv[
                (MaStR_konv.Nettonennleistung<= K_NEP * (1+buffer_capacity))
                & (MaStR_konv.Nettonennleistung>= K_NEP * (1-buffer_capacity))
                & (MaStR_konv.plz_Ma==int(plz_NEP))
                & MaStR_konv.energietraeger_Ma.isin(carrier)]

            if len(selected) > 0:
                chp_NEP_matched = chp_NEP_matched.append(
                    geopandas.GeoDataFrame(
                        data = {
                            'source': 'MaStR scaled with NEP 2021 list',
                            'MaStRNummer': selected.EinheitMastrNummer.head(1),
                            'carrier': (
                                carrier_egon[ET] if row.c2035_chp=='Nein'
                                else 'gas'),
                            'chp': True,
                            'el_capacity': row.c2035_capacity,
                            'th_capacity': selected.ThermischeNutzleistung.head(1),
                            'scenario': 'eGon2035',
                            'geometry': selected.geometry.head(1),
                            'voltage_level': selected.voltage_level.head(1)
                        }))
                chp_NEP = chp_NEP.drop(index)
                MaStR_konv = MaStR_konv.drop(selected.index)

    return chp_NEP_matched, MaStR_konv, chp_NEP

def match_chp(chp_NEP, MaStR_konv, chp_NEP_matched, consider_carrier=True):

    map_carrier = pd.Series(
        data = {
        'Kuppelgase': ['Erdgas', 'AndereGase','Mineraloelprodukte' ],
        'Sonstige_Energietraeger':[
                    'Erdgas', 'AndereGase','Mineraloelprodukte',
                    'Waerme','NichtBiogenerAbfall'],
        'Mineraloelprodukte':['Erdgas', 'Mineraloelprodukte' ],
        'Erdgas': ['Erdgas'],
        'Steinkohle': ['Steinkohle']})

    carrier_egon = map_carrier_egon_mastr()

    for i, row in chp_NEP.iterrows():
        if consider_carrier:
            # Select MaStR power plants with the same carrier and PLZ
            selected_plants = MaStR_konv[
                            (MaStR_konv.energietraeger_Ma.isin(
                                map_carrier[row.carrier]))
                            &(MaStR_konv.plz_Ma==int(row.postcode))
                            &(MaStR_konv.Nettonennleistung>50)]
        else:
            # Select MaStR power plants with the same PLZ
            selected_plants = MaStR_konv[
                            (MaStR_konv.plz_Ma==int(row.postcode))
                            &(MaStR_konv.Nettonennleistung>50)]

        selected_plants.loc[:, 'Nettonennleistung'] = (
                        row.c2035_capacity * selected_plants.Nettonennleistung/
                        selected_plants.Nettonennleistung.sum())
        chp_NEP_matched = chp_NEP_matched.append(
            geopandas.GeoDataFrame(
                data = {
                    'source': 'MaStR scaled with NEP 2021',
                    'MaStRNummer': selected_plants.EinheitMastrNummer,
                    'carrier': (
                        carrier_egon[row.carrier] if row.c2035_chp=='Nein'
                        else 'gas'),
                    'chp': True,
                    'el_capacity': selected_plants.Nettonennleistung,
                    'th_capacity': selected_plants.ThermischeNutzleistung,
                    'scenario': 'eGon2035',
                    'geometry': selected_plants.geometry,
                    'voltage_level': selected_plants.voltage_level.mean()
                    }))
        if len(selected_plants) > 0:
            chp_NEP = chp_NEP.drop(i)
            MaStR_konv = MaStR_konv.drop(selected_plants.index)

    return chp_NEP_matched, chp_NEP, MaStR_konv

################################################### Final table ###################################################
def insert_chp_egon2035():

    target = config.datasets()["chp_location"]["targets"]["power_plants"]

    chp_NEP = select_chp_from_nep()

    MaStR_konv = select_chp_from_mastr()

    # Assign voltage level
    MaStR_konv['voltage_level'] = assign_voltage_level(
        MaStR_konv, config.datasets()["chp_location"])

    chp_NEP_matched = geopandas.GeoDataFrame(
        columns = [
            'carrier','chp','el_capacity','th_capacity', 'scenario','geometry',
            'MaStRNummer', 'source', 'voltage_level'])

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1)

    MaStR_konv = MaStR_konv.groupby(
        ['plz_Ma', 'longitude', 'latitude','energietraeger_Ma', 'voltage_level']
        )[['Nettonennleistung', 'ThermischeNutzleistung', 'EinheitMastrNummer'
           ]].sum(numeric_only=False).reset_index()

    MaStR_konv['geometry'] = geopandas.points_from_xy(
        MaStR_konv['longitude'], MaStR_konv['latitude'])

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1)

    chp_NEP_matched, chp_NEP, MaStR_konv = match_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, consider_carrier=True)

    chp_NEP_matched, chp_NEP, MaStR_konv = match_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, consider_carrier=False)

    chp_NEP_matched["geometry_wkt"] = chp_NEP_matched["geometry"].apply(
        lambda geom: geom.wkt)

    print(f"{chp_NEP_matched.el_capacity.sum()} MW matched")
    print(f"{chp_NEP.c2035_capacity.sum()} MW not matched")

    # Aggregate chp per location and carrier
    insert_chp = chp_NEP_matched.groupby(["carrier", "geometry_wkt", "voltage_level"])[
        ['el_capacity', 'th_capacity', 'geometry',
           'MaStRNummer', 'source']].sum(numeric_only=False).reset_index()
    insert_chp.loc[:, 'geometry'] = chp_NEP_matched.set_index('geometry_wkt').loc[
        insert_chp.set_index('geometry_wkt').index, 'geometry'].unique()

    # Assign bus_id
    insert_chp['bus_id'] = assign_bus_id(
        insert_chp, config.datasets()["chp_location"]).bus_id

    db.execute_sql(
        f"""DELETE FROM {target['schema']}.{target['table']}
        WHERE carrier IN ('gas', 'other_non_renewable', 'oil')
        AND scenario='eGon2035'""")

    session = sessionmaker(bind=db.engine())()

    for i, row in insert_chp.iterrows():
        entry = EgonPowerPlants(
                sources={
                    "chp": "MaStR",
                    "el_capacity": row.source,
                    "th_capacity": "MaStR",
                },
                source_id={"MastrNummer": row.MaStRNummer},
                carrier=row.carrier,
                chp=True,
                el_capacity=row.el_capacity,
                th_capacity= row.th_capacity,
                voltage_level = row.voltage_level,
                bus_id = row.bus_id,
                scenario='eGon2035',
                geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
            )
        session.add(entry)
    session.commit()

