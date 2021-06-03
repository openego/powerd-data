"""The central module containing all code dealing with heat supply
for district heating areas.

"""
import pandas as pd
import geopandas as gpd
from egon.data import db, config

from egon.data.datasets.heat_supply.geothermal import calc_geothermal_costs

def capacity_per_district_heating_category(district_heating_areas, scenario):
    """ Calculates target values per district heating category and technology

    Parameters
    ----------
    district_heating_areas : geopandas.geodataframe.GeoDataFrame
        District heating areas per scenario
    scenario : str
        Name of the scenario

    Returns
    -------
    capacity_per_category : TYPE
        DESCRIPTION.

    """
    sources = config.datasets()['heat_supply']['sources']

    target_values = db.select_dataframe(
        f"""
        SELECT capacity, split_part(carrier, 'urban_central_', 2) as technology
        FROM {sources['scenario_capacities']['schema']}.
        {sources['scenario_capacities']['table']}
        WHERE carrier IN (
            'urban_central_heat_pump',
            'urban_central_resistive_heater',
            'urban_central_geo_thermal',
            'urban_central_solar_thermal_collector')
        """,
        index_col='technology')


    capacity_per_category = pd.DataFrame(
        index=['small', 'medium', 'large'],
        columns=['solar_thermal_collector',
                 'heat_pump', 'geo_thermal', 'demand'])

    capacity_per_category.demand = district_heating_areas.groupby(
        district_heating_areas.category).demand.sum()

    capacity_per_category.loc[
        ['small', 'medium'],'solar_thermal_collector'] = (
            target_values.capacity['solar_thermal_collector']
            *capacity_per_category.demand
            /capacity_per_category.demand[['small', 'medium']].sum())

    capacity_per_category.loc[:, 'heat_pump'] = (
        target_values.capacity['heat_pump']
        *capacity_per_category.demand
        /capacity_per_category.demand.sum())

    capacity_per_category.loc['large', 'geo_thermal'] = (
        target_values.capacity['geo_thermal'])

    return capacity_per_category


def set_technology_data():
    """Set data per technology according to Kurzstudie KWK

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    return  pd.DataFrame(
        index = ['CHP', 'solar_thermal_collector',
                 'heat_pump', 'geo_thermal'],
        columns = ['estimated_flh', 'priority'],
        data = {
            'estimated_flh': [8760, 1330, 7000, 3000],
            'priority': [4, 2 ,1 ,3]})


def select_district_heating_areas(scenario):
    """ Selects district heating areas per scenario and assigns size-category

    Parameters
    ----------
    scenario : str
        Name of the scenario

    Returns
    -------
    district_heating_areas : geopandas.geodataframe.GeoDataFrame
        District heating areas per scenario

    """

    sources = config.datasets()['heat_supply']['sources']

    max_demand_medium_district_heating = 96000

    max_demand_small_district_heating = 2400

    district_heating_areas = db.select_geodataframe(
         f"""
         SELECT id as district_heating_id,
         residential_and_service_demand as demand,
         geom_polygon as geom
         FROM {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
         WHERE scenario = '{scenario}'
         """,
         index_col='district_heating_id')

    district_heating_areas['category'] = 'large'

    district_heating_areas.loc[
        district_heating_areas[
            district_heating_areas.demand
            < max_demand_medium_district_heating].index,
        'category'] = 'medium'

    district_heating_areas.loc[
        district_heating_areas[
            district_heating_areas.demand
            < max_demand_small_district_heating].index,
        'category'] = 'small'

    return district_heating_areas


def cascade_per_technology(
        areas, technologies, capacity_per_category, size_dh,
        max_geothermal_costs = 2):

    """ Add plants of one technology suppliing district heating

    Parameters
    ----------
    areas : geopandas.geodataframe.GeoDataFrame
        District heating areas which need to be supplied
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    capacity_per_category : pandas.DataFrame
        Target installed capacities per size-category
    size_dh : str
        Category of the district heating areas
    max_geothermal_costs : float, optional
        Maxiumal costs of MW geothermal in EUR/MW. The default is 2.

    Returns
    -------
    areas : geopandas.geodataframe.GeoDataFrame
        District heating areas which need addistional supply technologies
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    append_df : pandas.DataFrame
        List of plants per district heating grid for the selected technology

    """
    sources = config.datasets()['heat_supply']['sources']

    tech = technologies[technologies.priority==technologies.priority.max()]

    if tech.index == 'CHP':

        gdf_chp = db.select_geodataframe(
            f"""SELECT id, geom, th_capacity as capacity
            FROM {sources['power_plants']['schema']}.
            {sources['power_plants']['table']}
            WHERE chp = True""")

        join = gpd.sjoin(gdf_chp.to_crs(4326), areas, rsuffix='area')

        append_df = pd.DataFrame(
            join.groupby('index_area').capacity.sum()).reset_index().rename(
                {'index_area': 'district_heating_id'}, axis=1)

    if tech.index in ['solar_thermal_collector', 'heat_pump', 'geo_thermal']:

        if tech.index == 'geo_thermal':

            gdf_geothermal = calc_geothermal_costs(max_geothermal_costs)

            join = gpd.sjoin(
                gdf_geothermal.to_crs(4326), areas, rsuffix='area')

            share_per_area = (
                join.groupby('index_area')['remaining_demand'].sum()/
                join['remaining_demand'].sum().sum())

        else:
            share_per_area = (
                areas['remaining_demand']/areas['remaining_demand'].sum())

        append_df = pd.DataFrame(
            (share_per_area).mul(
                capacity_per_category.loc[size_dh, tech.index].values[0]
                )).reset_index()

        append_df.rename({
            'index_area':'district_heating_id',
            'remaining_demand':'capacity'}, axis = 1, inplace=True)

    if append_df.size > 0:
        append_df['carrier'] = tech.index[0]
        append_df['category'] = size_dh
        areas.loc[append_df.district_heating_id,
                  'remaining_demand'] -= append_df.set_index(
                      'district_heating_id').capacity.mul(
                          tech.estimated_flh.values[0])

    areas = areas[areas.remaining_demand>=0]

    technologies = technologies.drop(tech.index)

    return areas, technologies, append_df


def cascade_heat_supply(scenario, plotting=True):
    """Assigns supply strategy for ditsrict heating areas.

    Different technologies are selected for three categories of district
    heating areas (small, medium and large annual demand).
    The technologies are priorized according to
    Flexibilisierung der Kraft-Wärme-Kopplung; 2017;
    Forschungsstelle für Energiewirtschaft e.V. (FfE)

    Parameters
    ----------
    scenario : str
        Name of scenario
    plotting : bool, optional
        Choose if district heating supply is plotted. The default is True.

    Returns
    -------
    resulting_capacities : pandas.DataFrame
        List of plants per district heating grid

    """

    # Select district heating areas from database
    district_heating_areas = select_district_heating_areas(scenario)

    # Select technolgies per district heating size
    map_dh_technologies = {
        'small': ['CHP', 'solar_thermal_collector', 'heat_pump'],
        'medium': ['CHP', 'solar_thermal_collector', 'heat_pump'],
        'large': ['CHP', 'geo_thermal', 'heat_pump'],
        }

    # Assign capacities per district heating category
    capacity_per_category = capacity_per_district_heating_category(
        district_heating_areas, scenario)

    # Initalize Dataframe for results
    resulting_capacities = pd.DataFrame(
        columns=['district_heating_id', 'carrier', 'capacity', 'category'])

    # Set technology data according to Kurzstudie KWK, NEP 2021
    technology_data = set_technology_data()

    for size_dh in ['small', 'medium', 'large']:

        areas = district_heating_areas[
            district_heating_areas.category==size_dh].to_crs(4326)

        areas['remaining_demand'] = areas['demand']

        technologies = technology_data.loc[map_dh_technologies[size_dh], :]

        while (len(technologies) > 0) and (len(areas) > 0):

            areas, technologies, append_df = cascade_per_technology(
                areas, technologies, capacity_per_category, size_dh)

            resulting_capacities = resulting_capacities.append(
                append_df, ignore_index=True)

    if plotting:
        plot_heat_supply(resulting_capacities)

    return gpd.GeoDataFrame(
        resulting_capacities,
        geometry = district_heating_areas.geom[
            resulting_capacities.district_heating_id].centroid.values)


def plot_heat_supply(resulting_capacities):

    from matplotlib import pyplot as plt

    district_heating_areas = select_district_heating_areas('eGon2035')

    for c in ['CHP', 'solar_thermal_collector', 'geo_thermal', 'heat_pump']:
        district_heating_areas[c] = resulting_capacities[
            resulting_capacities.carrier==c].set_index(
                'district_heating_id').capacity

        fig, ax = plt.subplots(1, 1)
        district_heating_areas.boundary.plot(linewidth=0.2,ax=ax, color='black')
        district_heating_areas.plot(
            ax=ax,
            column=c,
            cmap='magma_r',
            legend=True,
            legend_kwds={'label': f"Installed {c} in MW",
                         'orientation': "vertical"})
        plt.savefig(f'plots/heat_supply_{c}.png', dpi=300)