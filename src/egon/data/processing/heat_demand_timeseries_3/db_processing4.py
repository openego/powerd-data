import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as sqlio
import geopandas as gpd
import matplotlib.pyplot as plt
import os
import glob
from egon.data import db
import xarray as xr

import atlite
import netCDF4
from netCDF4 import Dataset

from math import ceil

index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

###generate temperature interval for each temperature zone
class IdpProfiles:    
    def __init__(self,df_index, **kwargs):
        
        self.df = pd.DataFrame(index = df_index)
        
        self.temperature = kwargs.get('temperature')
       
   #combination of weighted_temperature, get_normalized_bdew and temperature_inteval
    def get_temperature_interval(self, how='geometric_series'):
        """Appoints the corresponding temperature interval to each temperature
        in the temperature vector.
        """
        self.df['temperature']=self.temperature.values        
        
        temperature =self.df['temperature'].resample('D').mean().reindex(
            self.df.index).fillna(method='ffill').fillna(method='bfill')
        
        if how == 'geometric_series':
            temperature_mean = (temperature + 0.5 * np.roll(temperature, 24) +
                                    0.25 * np.roll(temperature, 48) +
                                    0.125 * np.roll(temperature, 72)) / 1.875
        elif how == 'mean':
            temperature_mean = temperature
        
        else:
            temperature_mean = None
        
        self.df['temperature_geo']= temperature_mean
        
        temperature_rounded=[]
        
        for i in self.df['temperature_geo']: 
            temperature_rounded.append(ceil(i))
        
        intervals = ({
            -20: 1, -19: 1, -18: 1, -17: 1, -16: 1, -15: 1, -14: 2,
            -13: 2, -12: 2, -11: 2, -10: 2, -9: 3, -8: 3, -7: 3, -6: 3, -5: 3,
            -4: 4, -3: 4, -2: 4, -1: 4, 0: 4, 1: 5, 2: 5, 3: 5, 4: 5, 5: 5,
            6: 6, 7: 6, 8: 6, 9: 6, 10: 6, 11: 7, 12: 7, 13: 7, 14: 7, 15: 7,
            16: 8, 17: 8, 18: 8, 19: 8, 20: 8, 21: 9, 22: 9, 23: 9, 24: 9,
            25: 9, 26: 10, 27: 10, 28: 10, 29: 10, 30: 10, 31: 10, 32: 10,
            33: 10, 34: 10, 35: 10, 36: 10, 37: 10, 38: 10, 39: 10, 40: 10})
        
        temperature_interval=[] 
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])
        
        self.df['temperature_interval'] = temperature_interval
        
        return self.df

##extracting the temperature for each temperature zone from cds data
def temperature_profile_extract():
    #downloading all data for Germany
    # load cutout
        cutout = atlite.Cutout("Germany-2011-era5",
                                cutout_dir = 'cutouts',
                                module="era5",
                                xs= slice(5., 16.), 
                                ys=slice(46., 56.), 
                                years= slice(2011, 2011)
                                )
        cutout.prepare()
        #file in the nextcloud shared link
        #will be directly taken from the dataframe??
        #os.chdir(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/atlite/cutouts/Germany-2011-era5')
        ##### replace this with input from the dataframe
        temperature_profile=pd.DataFrame()
        for file in glob.glob('*.nc'):        
            weather_data_raw = xr.open_dataset(file)
            temperature_raw = weather_data_raw.temperature.values
            index = weather_data_raw.indexes._indexes
            lats= index['y']
            lon = index['x']
            time =index['time']
            weather_data = np.zeros(shape=(temperature_raw.size, 4))
            count = 0
            
            for hour in range(index['time'].size):
                for row in range(index['y'].size):
                    for column in range(index['x'].size):
                        weather_data[count, 0] = hour
                        weather_data[count, 1] = index['y'][row]
                        weather_data[count, 2] = index['x'][column]
                        weather_data[count, 3] = temperature_raw[hour, row, column] - 273.15
                        count += 1
            weather_df = pd.DataFrame(weather_data)
            temperature_month = pd.DataFrame()
            
            ##csv file available in the nextcloud folder
            #path = '/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/TRY_Climate_Zones'
            station_location=pd.read_csv(os.path.join(os.getcwd(),'TRY_Climate_Zones','station_coordinates.csv'))
            #station_location=pd.read_csv(os.path.join(path,'station_coordinates.csv'))
            
            for row in station_location.index:
                station_name=station_location.iloc[row,0]            
                longitude_station = station_location.iloc[row,1]
                latitude_station = station_location.iloc[row,2]
                sq_diff_lat =(lats-latitude_station)**2
                sq_diff_lon = (lon-longitude_station)**2
                min_index_lat = sq_diff_lat.argmin()
                min_index_lon = sq_diff_lon.argmin()
                current_cut = weather_df[(weather_df[1]==index['y'][min_index_lat]) & (weather_df[2]==index['x'][min_index_lon])]
                current_cut.set_index(time, inplace =True)
                df= pd.DataFrame(0, columns = [station_name], index = time)
                df[station_name]=current_cut.iloc[:,3]
                temperature_month=pd.concat([temperature_month,df],axis=1)
        
            temperature_profile = temperature_profile.append(temperature_month)
        
        temperature_profile.sort_index(inplace=True)
        
        return temperature_profile

###generate temperature zones for each cell
def temp_interval():    
    temperature_interval = pd.DataFrame()
    temp_profile = temperature_profile_extract()
    
    for x in range(len(temp_profile.columns)):
        name_station = temp_profile.columns[x]
        idp_this_station = IdpProfiles(index, temperature=temp_profile[temp_profile.columns[x]]).get_temperature_interval(how='geometric_series')
        temperature_interval[name_station]=idp_this_station['temperature_interval']
    
    return temperature_interval

###generate idp pool from the profiles generated from the load profile generator
def idp_pool_generator():
    ###read hdf5 files with the generated profiles from the load profile generator
    #path = os.path.join(r'/home/student/Documents/egon_AM/heat_demand_generation/idp pool generation',
                        'heat_data.hdf5')
    path = os.path.join(os.path.join(os.getcwd(), 'heat_data.hdf5'))

    index = pd.date_range(pd.datetime(2011, 1, 1, 0), periods=8760, freq='H')

    sfh = pd.read_hdf(path, key ='SFH')
    mfh = pd.read_hdf(path, key ='MFH')
    temp = pd.read_hdf(path, key ='temperature')
    
    ######can wuerzburg file directly be added into the hdf5 file????
    #temp_wuerzburg = pd.read_csv(os.path.join(r'/home/student/Documents/egon_AM/feb_analysis/5_03',
                                              'temp_2011_Wuerzburg.csv'))
    temp_wuerzburg = pd.read_csv(os.path.join(os.path.join(os.getcwd(),'temp_2011_Wuerzburg.csv')))
    temp_wuerzburg.drop(columns ='Unnamed: 0', inplace=True)
    temp_wuerzburg.set_index(index, inplace=True)
    temp_wuerzburg.rename(columns = {'temperature':'Wuerzburg'}, inplace=True)
    temp = pd.concat([temp,temp_wuerzburg], axis =1)
    
    ##############################################################################
    #demand per city
    globals()['luebeck_sfh'] = sfh[sfh.filter(like='Luebeck').columns]
    globals()['luebeck_mfh'] = mfh[mfh.filter(like='Luebeck').columns]
    
    globals()['kassel_sfh'] = sfh[sfh.filter(like='Kassel').columns]
    globals()['kassel_mfh'] = mfh[mfh.filter(like='Kassel').columns]
   
    globals()['wuerzburg_sfh'] = sfh[sfh.filter(like='Wuerzburg').columns]
    globals()['wuerzburg_mfh'] = mfh[mfh.filter(like='Wuerzburg').columns]
    
    
    ####dataframe with daily temperature in geometric series
    temp_daily = pd.DataFrame()
    for column in temp.columns:
        temp_current= temp[column].resample('D').mean().reindex(temp.index).fillna(method='ffill').fillna(method='bfill')
        temp_daily=pd.concat([temp_daily,temp_current], axis=1)
        
    def round_temperature(station):
        intervals = ({
            -20: 1, -19: 1, -18: 1, -17: 1, -16: 1, -15: 1, -14: 2,
            -13: 2, -12: 2, -11: 2, -10: 2, -9: 3, -8: 3, -7: 3, -6: 3, -5: 3,
            -4: 4, -3: 4, -2: 4, -1: 4, 0: 4, 1: 5, 2: 5, 3: 5, 4: 5, 5: 5,
            6: 6, 7: 6, 8: 6, 9: 6, 10: 6, 11: 7, 12: 7, 13: 7, 14: 7, 15: 7,
            16: 8, 17: 8, 18: 8, 19: 8, 20: 8, 21: 9, 22: 9, 23: 9, 24: 9,
            25: 9, 26: 10, 27: 10, 28: 10, 29: 10, 30: 10, 31: 10, 32: 10,
            33: 10, 34: 10, 35: 10, 36: 10, 37: 10, 38: 10, 39: 10, 40: 10})
        temperature_rounded=[]
        for i in temp_daily.loc[:,station]:
            temperature_rounded.append(ceil(i))
        temperature_interval =[]
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])
        temp_class_dic = {f'Class_{station}': temperature_interval}
        temp_class = pd.DataFrame.from_dict(temp_class_dic)
        return temp_class
    
    temp_class_luebeck = round_temperature('Luebeck')
    temp_class_kassel = round_temperature('Kassel')
    temp_class_wuerzburg = round_temperature('Wuerzburg')
    temp_class =pd.concat([temp_class_luebeck, temp_class_kassel,temp_class_wuerzburg],axis=1)
    temp_class.set_index(index, inplace=True)  
       
    def unique_classes(station):
        classes=[]
        for x in temp_class[f'Class_{station}']:
            if x not in classes:
                classes.append(x)       
        classes.sort()
        return classes
        
    globals()['luebeck_classes'] = unique_classes('Luebeck')
    globals()['kassel_classes'] = unique_classes('Kassel')
    globals()['wuerzburg_classes'] = unique_classes('Wuerzburg')
      
    stock=['MFH','SFH']
    class_list=[3,4,5,6,7,8,9,10] 
    for s in stock:
         for m in class_list:
             globals()[f'idp_collection_class_{m}_{s}']=pd.DataFrame(index=range(24))  
    

    def splitter(station,household_stock):
        this_classes = globals()[f'{station.lower()}_classes']
        for classes in this_classes:
            this_itteration = globals()[f'{station.lower()}_{household_stock.lower()}'].loc[temp_class[f'Class_{station}']==classes,:]
            days = list(range(int(len(this_itteration)/24)))
            for day in days:
                this_day = this_itteration[day*24:(day+1)*24]
                this_day = this_day.reset_index(drop=True)
                globals()[f'idp_collection_class_{classes}_{household_stock}']=pd.concat(
                                [globals()[f'idp_collection_class_{classes}_{household_stock}'],
                                                                            this_day],axis=1,ignore_index=True)
    splitter('Luebeck','SFH')
    splitter('Kassel','SFH')
    splitter('Wuerzburg','SFH')
    splitter('Luebeck','MFH')
    splitter('Kassel','MFH')
    splitter('Wuerzburg','MFH')
    
    def pool_normalize(x):
            if x.sum()!=0:
                c=x.sum()
                return (x/c)
            else:
                return x
   
    stock=['MFH','SFH']
    class_list=[3,4,5,6,7,8,9,10] 
    for s in stock:
         for m in class_list:
             df_name = globals()[f'idp_collection_class_{m}_{s}']
             globals()[f'idp_collection_class_{m}_{s}_norm']=df_name.apply(pool_normalize)    
    
    return [idp_collection_class_3_SFH_norm,idp_collection_class_4_SFH_norm,idp_collection_class_5_SFH_norm,
            idp_collection_class_6_SFH_norm,idp_collection_class_7_SFH_norm,idp_collection_class_8_SFH_norm,
            idp_collection_class_9_SFH_norm,idp_collection_class_10_SFH_norm,idp_collection_class_3_MFH_norm,
            idp_collection_class_4_MFH_norm,idp_collection_class_5_MFH_norm, idp_collection_class_6_MFH_norm,
            idp_collection_class_7_MFH_norm,idp_collection_class_8_MFH_norm,idp_collection_class_9_MFH_norm,
            idp_collection_class_10_MFH_norm]

#convert the multiple idp pool into a single dataframe
def idp_df_generator():
    idp_list = idp_pool_generator()
    stock=['MFH','SFH']
    class_list=[3,4,5,6,7,8,9,10] 
    idp_df = pd.DataFrame(columns=['idp', 'house', 'temperature_class'])
    for s in stock:
        for m in class_list:
            #var_name=globals()[f'idp_collection_class_{m}_{s}']
            if s =='SFH':
              i = class_list.index(m)
            if s == 'MFH':
              i = class_list.index(m) + 8
            current_pool = idp_list[i]
            idp_df = idp_df.append(
                pd.DataFrame(
                    data = {
                        'idp': current_pool.transpose().values.tolist(),
                        'house': s,
                        'temperature_class': m
                        }))
    idp_df = idp_df.reset_index(drop=True)
    idp_df['idp'] =idp_df.idp.apply(lambda x: np.array(x)) 
    
    ####write to the database
        
    return idp_df 
    

##extracting and adjusting the demand data to obtain desired structure
## considering only residential for 2035 scenario
def annual_demand_generator():      
    # function for extracting demand data from pgadmin airflow database(new one added on 3003); 
    #converts the table into a dataframe   
    def psycop_df_AF(table_name):
        conn = db.engine()
        sql = "SELECT * FROM {}".format(table_name)
        data = sqlio.read_sql_query(sql, conn)
        conn = None
        return data
    # the function extracts all spatial data in the form of geopandas dataframe      
    def psycop_gdf_AF(table_name):
        conn = db.engine()
        sql = "SELECT * FROM {}".format(table_name)
        data = gpd.read_postgis(sql, conn)
        conn = None
        return data
           
    demand = psycop_df_AF('demand.egon_peta_heat')
    a_pha = psycop_df_AF('society.destatis_zensus_apartment_per_ha')
    b_pha = psycop_df_AF('society.destatis_zensus_building_per_ha')
    h_pha = psycop_df_AF('society.destatis_zensus_household_per_ha')
    
    zp_pha = psycop_gdf_AF('society.destatis_zensus_population_per_ha')
    
    ##considering only eGon2035 demand scenario for the residential sector
    demand = demand[demand['scenario'] == 'eGon2035']
    demand = demand[demand['sector'] == 'residential']
    
    a_pha_1 = a_pha[['zensus_population_id', 'grid_id']]
    a_pha_1 = a_pha_1.drop_duplicates(subset='zensus_population_id')
    b_pha_1 = b_pha[['zensus_population_id', 'grid_id']]
    b_pha_1 = b_pha_1.drop_duplicates(subset='zensus_population_id')
    h_pha_1 = h_pha[['zensus_population_id', 'grid_id']]
    h_pha_1 = h_pha_1.drop_duplicates(subset='zensus_population_id')
    
    all_grid = a_pha_1.append(b_pha_1).append(h_pha_1)
    all_grid = all_grid.drop_duplicates(subset='zensus_population_id')
    demand_grid = pd.merge(demand, all_grid, how='inner',
                           on='zensus_population_id')
    
    all_geom = zp_pha.drop(zp_pha.columns.difference(['grid_id', 'geom']), axis=1)
    demand_geom = pd.merge(demand_grid, all_geom, how='inner', on='grid_id')
    
    demand_geom = gpd.GeoDataFrame(demand_geom, geometry='geom')
    demand_geom = demand_geom.drop(
        demand_geom.columns.difference(['demand', 'grid_id', 'geom','zensus_population_id']), 1)
    demand_geom['geom'] = demand_geom['geom'].to_crs(epsg=4326)
    ####import demand geo ###shape file available in the next cloud link
    #temperature_zones = gpd.read_file(
        '/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/TRY_Climate_Zones/Try_Climate_Zone.shp')###change this file location
    temperature_zones = gpd.read_file(os.path.join(os.getcwd(),'TRY_Climate_Zones','Try_Climate_Zone.shp'))
    temperature_zones.sort_values('Zone', inplace=True)
    temperature_zones.reset_index(inplace=True)
    temperature_zones.drop(columns=['index', 'Id'], inplace=True, axis=0)
    #####import temperature_zones  
    
    demand_zone = gpd.sjoin(demand_geom, temperature_zones,
                            how='inner', op='intersects')

    # 300 repeated overrlapping cells removed
    demand_zone.drop_duplicates(['grid_id'], inplace=True)
    
    ##classification of zensus 'characteritic_text' into household stock categories
    sfh_chartext = ['Freistehendes Einfamilienhaus', 'Einfamilienhaus: Doppelhaushälfte', 'Einfamilienhaus: Reihenhaus',
                    'Freistehendes Zweifamilienhaus', 'Zweifamilienhaus: Doppelhaushälfte', 'Zweifamilienhaus: Reihenhaus']
    mfh_chartext = ['Mehrfamilienhaus: 3-6 Wohnungen', 'Mehrfamilienhaus: 7-12 Wohnungen',
                    'Mehrfamilienhaus: 13 und mehr Wohnungen', 'Anderer Gebäudetyp']
    
    ###only builing types with attribute GEBTYPGROESSE considered for household stock characterization
    bg_pha = b_pha[b_pha['attribute'] == 'GEBTYPGROESSE']
    
    ##assigning household stock to each cell
    def household_stock(x):
        if x in sfh_chartext:
            output = 'SFH'
        if x in mfh_chartext:
            output = 'MFH'
        return output    
    bg_pha['Household Stock'] = bg_pha['characteristics_text'].apply(
        household_stock)
    ##counting sfh and mfh for each cell
    house_count = bg_pha[['zensus_population_id', 'quantity', 'Household Stock']]
    house_count = house_count.groupby(
        ['zensus_population_id', 'Household Stock']).sum('quantity')
    house_count = house_count.reset_index()
    house_count = house_count.pivot_table(
        values='quantity', index='zensus_population_id', columns='Household Stock')
    house_count = house_count.fillna(0)
    
    demand_count = pd.merge(demand_zone, house_count, how='inner', on='zensus_population_id')
    
    demand_count.drop('index_right', axis=1, inplace=True)
    #demand count consists of demand and household stock count for each cell
    
    
    ###assigning a array for each cell consiting of the day class. array length 365
    # all_temperature_interval is the output of HTS_generation.py
    # direct:
    all_temperature_interval = temp_interval()
    all_temperature_interval.set_index(pd.to_datetime(
        all_temperature_interval.index), inplace=True)
    all_temperature_interval = all_temperature_interval.resample('D').max()
    
    def interval_allocation(x):
        if x == 'Hamburg_Fuhlsbuettel':
            return np.array(all_temperature_interval['Hamburg_Fuhlsbuettel'])
        if x == 'Rostock_Warnemuende':
            return np.array(all_temperature_interval['Rostock_Warnemuende'])
        if x == 'Bremerhaven':
            return np.array(all_temperature_interval['Bremerhaven'])
        
        ####other station to be added for the whole database
    
    demand_count['Temperature_interval'] = demand_count['Station'].apply(
        interval_allocation)
    demand_count.drop(demand_count.columns.difference(
        ['zensus_population_id', 'demand', 'SFH', 'MFH', 'Temperature_interval','Station']), axis=1, inplace=True)
    
    return demand_count,all_temperature_interval ##df with demand,hhstock count and daily temprature interval



##generates dataframe with randomly selected column names from the idp pool
def profile_selector(station):
    idp_df = idp_df_generator()
    
    #idp_df = pd.read_pickle(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/idp_df.pickle')
    #idp_df = idp_df_sample #####################################to be read from the pgadmin database direclty
    annual_demand, all_temperature_interval = annual_demand_generator()
    #annual_demand = pd.read_pickle(r'/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/profile_selector_output_12.05/annual_demand.pickle')
    #all_temperature_interval = temp_interval()
    
    Temperature_interval = pd.DataFrame(columns = range(365))
   
    #all_temperature_interval.set_index(index,inplace=True)
    #all_temperature_interval = all_temperature_interval.resample('D').max() ###already resampled inside annual_demand_generator##delete this

    all_temperature_interval.reset_index(drop=True, inplace=True)
    
    all_temperature_interval = all_temperature_interval.iloc[:,0:3]#####only considering the saple

        
    Temperature_interval = all_temperature_interval.transpose()
             

    # Set seed value to have reproducable results
    np.random.seed(0)
    #generates a dataframe with the idp index number of the selected profiles for each temperature 

    #for station in Temperature_interval.index:
    result_SFH =pd.DataFrame(columns=Temperature_interval.columns)
    result_MFH =pd.DataFrame(columns=Temperature_interval.columns)
    for day in Temperature_interval.columns:
        t_class = Temperature_interval.loc[station,day].astype(int)
        
        array_SFH = np.array(idp_df[(idp_df.temperature_class==t_class)
                                    &(idp_df.house=='SFH')].index.values)
        
        array_MFH = np.array(idp_df[(idp_df.temperature_class==t_class)
                                    &(idp_df.house=='MFH')].index.values) 
        
        result_SFH[day] = np.random.choice(
            array_SFH, int(annual_demand[annual_demand.Station==station].SFH.sum()))
        
        result_MFH[day] = np.random.choice(
            array_MFH, int(annual_demand[annual_demand.Station==station].MFH.sum()))


     
    result_SFH['zensus_population_id'] = annual_demand[
        annual_demand.Station==station].loc[
            annual_demand[annual_demand.Station==station].index.repeat(
                annual_demand[annual_demand.Station==station].SFH.astype(int))].zensus_population_id.values

    result_MFH['zensus_population_id'] =annual_demand[
        annual_demand.Station==station].loc[
            annual_demand[annual_demand.Station==station].index.repeat(
                annual_demand[annual_demand.Station==station].MFH.astype(int))].zensus_population_id.values
    
    result_SFH.set_index('zensus_population_id',inplace=True)
    result_MFH.set_index('zensus_population_id',inplace=True)

    return [result_SFH,result_MFH]



##for building class 11, shlp_type =EFH, wind_impact=0, from shlp_sigmoid_factors.csv, demandlib
def h_value():
    a = 3.0469695
    
    b = -37.1833141
    
    c = 5.6727847
    
    d = 0.1163157
    
   
    temp_profile =temperature_profile_extract()
    temperature_profile_res =temp_profile.resample('D').mean().reindex(index).fillna(method='ffill').fillna(method='bfill')
    
    temp_profile_geom = ((temperature_profile_res.transpose() + 0.5 * np.roll(temperature_profile_res.transpose(), 24,axis=1) +
                                    0.25 * np.roll(temperature_profile_res.transpose(), 48,axis=1) +
                                    0.125 * np.roll(temperature_profile_res.transpose(), 72,axis=1)) / 1.875).transpose()
    
    #for each temperature station h value created for hourly resolution
    h= (a / (1 + (b / (temp_profile_geom - 40)) ** c) + d)
    return h
    
##generaing specific profiles of grid cell as per the assigned index
###### normalizing and scaling up of profiles
def profile_generator(station):
    #selected_profile=profile_selector()
    #x = globals()[f'result_MFH_{station}']
    selected_profiles = profile_selector()
    
    if station=='Bremerhaven':
        sfh=selected_profiles[0]
        mfh=slelected_profile[1]
    if station=='Hamburg_Fuhlsbuettel':
        sfh=selected_profiles[2]
        mfh=slelected_profile[3]
    if station=='Rostock_Warnemuende':
        sfh=selected_profiles[4]
        mfh=slelected_profile[5]
        
    idp_df = idp_df_generator()
    y = idp_df['idp'].reset_index()
    heat_profile_idp_sfh = pd.DataFrame(index=sfh.index)
    heat_profile_idp_mfh = pd.DataFrame(index=sfh.index)
    
    ##for SFH
    for i in sfh.columns:
        col=sfh[i]
        col=col.reset_index()
        y.rename(columns={'index':i},inplace=True)
        col=pd.merge(col,y[[i,'idp']],how='left', on=i)
        col[i]=col['idp']
        col.drop('idp',axis=1,inplace=True)
        col.set_index('zensus_population_id', inplace=True)
        #col=col.groupby(lambda x:x, axis=0).sum()
        #heat_profile_idp = col[i].values
        heat_profile_idp_sfh[i] = col[i].values
        y.rename(columns={i:'index'},inplace=True)
    heat_profile_idp_sfh = heat_profile_idp_sfh.groupby(lambda x:x, axis=0).sum()
    heat_profile_idp_sfh = heat_profile_idp_sfh.transpose()
    heat_profile_idp_sfh = heat_profile_idp_sfh.apply(lambda x: x.explode())
    heat_profile_idp_sfh.set_index(index,inplace=True)
    heat_profile_idp_sfh = heat_profile_idp_sfh.transpose()
    
    for i in mfh.columns:
        col=mfh[i]
        col=col.reset_index()
        y.rename(columns={'index':i},inplace=True)
        col=pd.merge(col,y[[i,'idp']],how='left', on=i)
        col[i]=col['idp']
        col.drop('idp',axis=1,inplace=True)
        col.set_index('zensus_population_id', inplace=True)
        #col=col.groupby(lambda x:x, axis=0).sum()
        #heat_profile_idp = col[i].values
        heat_profile_idp_mfh[i] = col[i].values
        y.rename(columns={i:'index'},inplace=True)
    heat_profile_idp_mfh = heat_profile_idp_mfh.groupby(lambda x:x, axis=0).sum()
    heat_profile_idp_mfh = heat_profile_idp_mfh.transpose()
    heat_profile_idp_mfh = heat_profile_idp_mfh.apply(lambda x: x.explode())
    heat_profile_idp_mfh.set_index(index,inplace=True)
    heat_profile_idp_mfh = heat_profile_idp_mfh.transpose()
    
    return heat_profile_idp_sfh, heat_profile_idp_mfh


def profile_district_heating_generator():
    #district_heating = pd.read_csv('/home/student/Documents/egon_AM/heat_demand_generation/Heat_time_series_all_files/phase4/map_district_heating_areas.csv')
    #imporrting distric heating table as csv; to be directly imported from database
    district_heating = pd.read_csv(os.path.join(os.getcwd(),'map_district_heating_areas.csv'))
    distric_heating = district_heating[district_heating['scenario']=='eGon2035']
    profile = profile_generator()
    profile = profile.transpose()
    district_profile= pd.merge(profile,district_heating[['area_id','zensus_population_id']],
             how='inner', left_on = profile.index,right_on ='zensus_population_id')
    district_profile.drop('zensus_population_id',axis=1,inplace=True)
    district_profile = district_profile.groupby(lambda x:x, axis=1).sum()
    
    district_demand = pd.merge(district_heating, annual_demand[['demand','zensus_population_id']], 
                              how='left', on='zensus_population_id')
    district_demand=district_demand[['area_id','demand']]
    district_demaand = district_demand.groupby('area_id').sum()
    
    

##scaling the profile as per the station h-value and the cell demand
def demand_scale(station):
    heat_demand_profile_sfh, heat_demand_profile_mfh = profile_generator(station)
    h = h_value()
    annual_demand = annual_demand_generator()
    
    #for SFH
    demand_curve_sfh = heat_demand_profile_sfh.multiply(h[station],axis=0)
    demand_curve_sfh = demand_curve_sfh.apply(lambda x: x/x.sum())
    heat_demand_sfh=demand_curve_sfh.transpose()
    heat_demand_sfh=heat_demand_sfh.reset_index()
    heat_demand_sfh.rename(columns={'index':'zensus_population_id'},inplace=True)
    heat_demand_sfh=pd.merge(heat_demand_sfh,annual_demand[['zensus_population_id','demand']],
                         how='inner', on='zensus_population_id')
        
    heat_demand.set_index('zensus_population_id', inplace=True)
    heat_demand=heat_demand[heat_demand.columns[:-1]].multiply(heat_demand.demand,axis=0)
    
    heat_demand=heat_demand.transpose()
    
    heat_demand.set_index(index,inplace=True)
    
    return heat_demand


    