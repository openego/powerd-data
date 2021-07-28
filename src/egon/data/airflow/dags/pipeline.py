import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import importlib_resources as resources

from egon.data.datasets import database
from egon.data.datasets.data_bundle import DataBundle
from egon.data.datasets.era5 import WeatherData
from egon.data.datasets.heat_etrago import HeatEtrago
from egon.data.datasets.heat_supply import HeatSupply
from egon.data.datasets.renewable_feedin import RenewableFeedin
from egon.data.datasets.osm import OpenStreetMap
from egon.data.datasets.mastr import mastr_data_setup
from egon.data.datasets.re_potential_areas import re_potential_area_setup
from egon.data.datasets.mv_grid_districts import mv_grid_districts_setup
from egon.data.datasets.power_plants import PowerPlants
from egon.data.datasets.vg250 import Vg250
from egon.data.processing.zensus_vg250 import (
    zensus_population_inside_germany as zensus_vg250,
)
import airflow
import egon.data.importing.demandregio as import_dr
import egon.data.importing.demandregio.install_disaggregator as install_dr
import egon.data.importing.etrago as etrago
import egon.data.importing.heat_demand_data as import_hd
import egon.data.importing.industrial_sites as industrial_sites

import egon.data.importing.nep_input_data as nep_input
import egon.data.importing.scenarios as import_scenarios
import egon.data.importing.zensus as import_zs
import egon.data.importing.gas_grid as gas_grid

import egon.data.processing.boundaries_grid_districts as boundaries_grid_districts
import egon.data.processing.demandregio as process_dr
import egon.data.processing.district_heating_areas as district_heating_areas
import egon.data.processing.osmtgmod as osmtgmod
import egon.data.processing.substation as substation
import egon.data.processing.zensus_vg250.zensus_population_inside_germany as zensus_vg250
import egon.data.processing.gas_areas as gas_areas
import egon.data.importing.scenarios as import_scenarios
import egon.data.importing.industrial_sites as industrial_sites
import egon.data.processing.loadarea as loadarea
import egon.data.processing.calculate_dlr as dlr

import egon.data.processing.zensus as process_zs
import egon.data.processing.zensus_grid_districts as zensus_grid_districts


from egon.data import db


with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
    template_searchpath=[
        os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "..", "processing", "vg250"
            )
        )
    ],
    is_paused_upon_creation=False,
    schedule_interval=None,
) as pipeline:

    tasks = pipeline.task_dict

    database_setup = database.Setup()
    database_setup.insert_into(pipeline)
    setup = tasks["database.setup"]

    osm = OpenStreetMap(dependencies=[setup])
    osm.insert_into(pipeline)
    osm_add_metadata = tasks["osm.add-metadata"]
    osm_download = tasks["osm.download"]

    data_bundle = DataBundle(dependencies=[setup])
    data_bundle.insert_into(pipeline)
    download_data_bundle = tasks["data_bundle.download"]

    # VG250 (Verwaltungsgebiete 250) data import
    vg250 = Vg250(dependencies=[setup])
    vg250.insert_into(pipeline)
    vg250_clean_and_prepare = tasks["vg250.cleaning-and-preperation"]

    # Zensus import
    zensus_download_population = PythonOperator(
        task_id="download-zensus-population",
        python_callable=import_zs.download_zensus_pop,
    )

    zensus_download_misc = PythonOperator(
        task_id="download-zensus-misc",
        python_callable=import_zs.download_zensus_misc,
    )

    zensus_tables = PythonOperator(
        task_id="create-zensus-tables",
        python_callable=import_zs.create_zensus_tables,
    )

    population_import = PythonOperator(
        task_id="import-zensus-population",
        python_callable=import_zs.population_to_postgres,
    )

    zensus_misc_import = PythonOperator(
        task_id="import-zensus-misc",
        python_callable=import_zs.zensus_misc_to_postgres,
    )
    setup >> zensus_download_population >> zensus_download_misc
    zensus_download_misc >> zensus_tables >> population_import
    vg250_clean_and_prepare >> population_import
    population_import >> zensus_misc_import

    # Combine Zensus and VG250 data
    map_zensus_vg250 = PythonOperator(
        task_id="map_zensus_vg250",
        python_callable=zensus_vg250.map_zensus_vg250,
    )

    zensus_inside_ger = PythonOperator(
        task_id="zensus-inside-germany",
        python_callable=zensus_vg250.inside_germany,
    )

    zensus_inside_ger_metadata = PythonOperator(
        task_id="zensus-inside-germany-metadata",
        python_callable=zensus_vg250.add_metadata_zensus_inside_ger,
    )

    vg250_population = PythonOperator(
        task_id="population-in-municipalities",
        python_callable=zensus_vg250.population_in_municipalities,
    )

    vg250_population_metadata = PythonOperator(
        task_id="population-in-municipalities-metadata",
        python_callable=zensus_vg250.add_metadata_vg250_gem_pop,
    )
    [
        vg250_clean_and_prepare,
        population_import,
    ] >> map_zensus_vg250 >> zensus_inside_ger >> zensus_inside_ger_metadata
    zensus_inside_ger >> vg250_population >> vg250_population_metadata

    # Scenario table
    scenario_input_tables = PythonOperator(
        task_id="create-scenario-parameters-table",
        python_callable=import_scenarios.create_table
    )

    scenario_input_import = PythonOperator(
        task_id="import-scenario-parameters",
        python_callable=import_scenarios.insert_scenarios
    )
    setup >> scenario_input_tables >> scenario_input_import

    # DemandRegio data import
    demandregio_tables = PythonOperator(
        task_id="demandregio-tables",
        python_callable=import_dr.create_tables,
    )

    scenario_input_tables >> demandregio_tables


    demandregio_installation = PythonOperator(
        task_id="demandregio-installation",
        python_callable=install_dr.clone_and_install,
    )

    setup >> demandregio_installation

    demandregio_society = PythonOperator(
        task_id="demandregio-society",
        python_callable=import_dr.insert_society_data,
    )

    demandregio_installation >> demandregio_society
    vg250_clean_and_prepare >> demandregio_society
    demandregio_tables >> demandregio_society
    scenario_input_import >> demandregio_society

    demandregio_demand_households = PythonOperator(
        task_id="demandregio-household-demands",
        python_callable=import_dr.insert_household_demand,
    )

    demandregio_installation >> demandregio_demand_households
    vg250_clean_and_prepare >> demandregio_demand_households
    demandregio_tables >> demandregio_demand_households
    scenario_input_import >> demandregio_demand_households

    demandregio_demand_cts_ind = PythonOperator(
        task_id="demandregio-cts-industry-demands",
        python_callable=import_dr.insert_cts_ind_demands,
    )

    demandregio_installation >> demandregio_demand_cts_ind
    vg250_clean_and_prepare >> demandregio_demand_cts_ind
    demandregio_tables >> demandregio_demand_cts_ind
    scenario_input_import >> demandregio_demand_cts_ind
    download_data_bundle >> demandregio_demand_cts_ind

    # Society prognosis
    prognosis_tables = PythonOperator(
        task_id="create-prognosis-tables",
        python_callable=process_zs.create_tables,
    )

    setup >> prognosis_tables

    population_prognosis = PythonOperator(
        task_id="zensus-population-prognosis",
        python_callable=process_zs.population_prognosis_to_zensus,
    )

    prognosis_tables >> population_prognosis
    map_zensus_vg250 >> population_prognosis
    demandregio_society >> population_prognosis
    population_import >> population_prognosis

    household_prognosis = PythonOperator(
        task_id="zensus-household-prognosis",
        python_callable=process_zs.household_prognosis_to_zensus,
    )
    prognosis_tables >> household_prognosis
    map_zensus_vg250 >> household_prognosis
    demandregio_society >> household_prognosis
    zensus_misc_import >> household_prognosis


    # Distribute electrical demands to zensus cells
    processed_dr_tables = PythonOperator(
        task_id="create-demand-tables",
        python_callable=process_dr.create_tables,
    )

    elec_household_demands_zensus = PythonOperator(
        task_id="electrical-household-demands-zensus",
        python_callable=process_dr.distribute_household_demands,
    )

    zensus_tables >> processed_dr_tables >> elec_household_demands_zensus
    population_prognosis >> elec_household_demands_zensus
    demandregio_demand_households >> elec_household_demands_zensus
    map_zensus_vg250 >> elec_household_demands_zensus

    # NEP data import
    create_tables = PythonOperator(
        task_id="create-scenario-tables",
        python_callable=nep_input.create_scenario_input_tables,
    )

    nep_insert_data = PythonOperator(
        task_id="insert-nep-data",
        python_callable=nep_input.insert_data_nep,
    )

    setup >> create_tables >> nep_insert_data
    vg250_clean_and_prepare >> nep_insert_data
    population_import >> nep_insert_data
    download_data_bundle >> nep_insert_data

    # setting etrago input tables
    etrago_input_data = PythonOperator(
        task_id="setting-etrago-input-tables",
        python_callable=etrago.setup,
    )
    setup >> etrago_input_data

    # Retrieve MaStR data
    mastr_data = mastr_data_setup(dependencies=[setup])
    mastr_data.insert_into(pipeline)
    retrieve_mastr_data = tasks["mastr.download-mastr-data"]

    # Substation extraction
    substation_tables = PythonOperator(
        task_id="create_substation_tables",
        python_callable=substation.create_tables,
    )

    substation_functions = PythonOperator(
        task_id="substation_functions",
        python_callable=substation.create_sql_functions,
    )

    hvmv_substation_extraction = PostgresOperator(
        task_id="hvmv_substation_extraction",
        sql=resources.read_text(substation, "hvmv_substation.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )

    ehv_substation_extraction = PostgresOperator(
        task_id="ehv_substation_extraction",
        sql=resources.read_text(substation, "ehv_substation.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )


    osm_add_metadata >> substation_tables >> substation_functions
    substation_functions >> hvmv_substation_extraction
    substation_functions >> ehv_substation_extraction
    vg250_clean_and_prepare >> hvmv_substation_extraction
    vg250_clean_and_prepare >> ehv_substation_extraction

    # osmTGmod ehv/hv grid model generation
    osmtgmod_osm_import = PythonOperator(
        task_id="osmtgmod_osm_import",
        python_callable=osmtgmod.import_osm_data,
    )

    run_osmtgmod = PythonOperator(
        task_id="run_osmtgmod",
        python_callable=osmtgmod.run_osmtgmod,
    )

    osmtgmod_pypsa = PythonOperator(
        task_id="osmtgmod_pypsa",
        python_callable=osmtgmod.osmtgmmod_to_pypsa,
    )

    osmtgmod_substation = PostgresOperator(
        task_id="osmtgmod_substation",
        sql=resources.read_text(osmtgmod, "substation_otg.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )

    osm_download >> osmtgmod_osm_import >> run_osmtgmod
    ehv_substation_extraction >> run_osmtgmod
    hvmv_substation_extraction >> run_osmtgmod
    run_osmtgmod >> osmtgmod_pypsa
    etrago_input_data >> osmtgmod_pypsa
    run_osmtgmod >> osmtgmod_substation

    # create Voronoi for MV grid districts
    create_voronoi_substation = PythonOperator(
        task_id="create-voronoi-substations",
        python_callable=substation.create_voronoi,
    )
    osmtgmod_substation >> create_voronoi_substation

    # MV grid districts
    mv_grid_districts = mv_grid_districts_setup(dependencies=[create_voronoi_substation])
    mv_grid_districts.insert_into(pipeline)
    define_mv_grid_districts = tasks["mv_grid_districts.define-mv-grid-districts"]

    # Import potential areas for wind onshore and ground-mounted PV
    re_potential_areas = re_potential_area_setup(dependencies=[setup])
    re_potential_areas.insert_into(pipeline)
    insert_re_potential_areas = tasks["re_potential_areas.insert-data"]

    # Future heat demand calculation based on Peta5_0_1 data
    heat_demand_import = PythonOperator(
        task_id="import-heat-demand",
        python_callable=import_hd.future_heat_demand_data_import,
    )
    vg250_clean_and_prepare >> heat_demand_import
    zensus_inside_ger_metadata >> heat_demand_import
    scenario_input_import >> heat_demand_import


    # Import and merge data on industrial sites from different sources

    industrial_sites_import = PythonOperator(
        task_id="download-import-industrial-sites",
        python_callable=industrial_sites.download_import_industrial_sites
    )

    industrial_sites_merge = PythonOperator(
        task_id="merge-industrial-sites",
        python_callable=industrial_sites.merge_inputs
    )

    industrial_sites_nuts = PythonOperator(
        task_id="map-industrial-sites-nuts3",
        python_callable=industrial_sites.map_nuts3
    )
    vg250_clean_and_prepare >> industrial_sites_import
    industrial_sites_import >> industrial_sites_merge >> industrial_sites_nuts

    # Distribute electrical CTS demands to zensus grid

    elec_cts_demands_zensus = PythonOperator(
        task_id="electrical-cts-demands-zensus",
        python_callable=process_dr.distribute_cts_demands,
    )

    processed_dr_tables >> elec_cts_demands_zensus
    heat_demand_import >> elec_cts_demands_zensus
    demandregio_demand_cts_ind >> elec_cts_demands_zensus
    map_zensus_vg250 >> elec_cts_demands_zensus


    # Gas grid import
    gas_grid_insert_data = PythonOperator(
        task_id="insert-gas-grid",
        python_callable=gas_grid.insert_gas_data,
    )

    etrago_input_data >> gas_grid_insert_data
    download_data_bundle >> gas_grid_insert_data

    # Create gas voronoi
    create_gas_polygons = PythonOperator(
        task_id="create-gas-voronoi",
        python_callable=gas_areas.create_voronoi,
    )

    gas_grid_insert_data  >> create_gas_polygons
    vg250_clean_and_prepare >> create_gas_polygons

    # Extract landuse areas from osm data set
    create_landuse_table = PythonOperator(
        task_id="create-landuse-table",
        python_callable=loadarea.create_landuse_table
    )

    landuse_extraction = PostgresOperator(
        task_id="extract-osm_landuse",
        sql=resources.read_text(loadarea, "osm_landuse_extraction.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    setup >> create_landuse_table
    create_landuse_table >> landuse_extraction
    osm_add_metadata >> landuse_extraction
    vg250_clean_and_prepare >> landuse_extraction

    # Import weather data
    weather_data = WeatherData(dependencies=[setup])
    download_weather_data = tasks["era5.download-era5"]

    renewable_feedin = RenewableFeedin(dependencies=[weather_data, vg250])

    feedin_wind_onshore = tasks["renewable_feedin.wind"]
    feedin_pv = tasks["renewable_feedin.pv"]
    feedin_solar_thermal = tasks["renewable_feedin.solar-thermal"]

    # District heating areas demarcation
    create_district_heating_areas_table = PythonOperator(
        task_id="create-district-heating-areas-table",
        python_callable=district_heating_areas.create_tables
    )
    import_district_heating_areas = PythonOperator(
        task_id="import-district-heating-areas",
        python_callable=district_heating_areas.
        district_heating_areas_demarcation
    )
    setup >> create_district_heating_areas_table
    create_district_heating_areas_table >> import_district_heating_areas
    zensus_misc_import >> import_district_heating_areas
    heat_demand_import >> import_district_heating_areas
    scenario_input_import >> import_district_heating_areas

    # Calculate dynamic line rating for HV trans lines
    calculate_dlr = PythonOperator(
        task_id="calculate_dlr",
        python_callable=dlr.Calculate_DLR,
    )
    osmtgmod_pypsa >> calculate_dlr
    download_data_bundle >> calculate_dlr
    download_weather_data >> calculate_dlr

    # Electrical load curves CTS
    map_zensus_grid_districts = PythonOperator(
        task_id="map_zensus_grid_districts",
        python_callable=zensus_grid_districts.map_zensus_mv_grid_districts,
    )
    population_import >> map_zensus_grid_districts
    define_mv_grid_districts >> map_zensus_grid_districts

    electrical_load_curves_cts = PythonOperator(
        task_id="electrical-load-curves-cts",
        python_callable=process_dr.insert_cts_load,
    )
    map_zensus_grid_districts >> electrical_load_curves_cts
    elec_cts_demands_zensus >> electrical_load_curves_cts
    demandregio_demand_cts_ind >> electrical_load_curves_cts
    map_zensus_vg250 >> electrical_load_curves_cts
    etrago_input_data >> electrical_load_curves_cts

    # Map federal states to mv_grid_districts
    map_boundaries_grid_districts = PythonOperator(
        task_id="map_vg250_grid_districts",
        python_callable=boundaries_grid_districts.map_mvgriddistricts_vg250,
    )
    define_mv_grid_districts >> map_boundaries_grid_districts
    vg250_clean_and_prepare >> map_boundaries_grid_districts

    # Power plants
    power_plants = PowerPlants(dependencies=[
        setup, renewable_feedin, mv_grid_districts, mastr_data,
        re_potential_areas])

    power_plant_import = tasks["power_plants.insert-hydro-biomass"]
    generate_wind_farms = tasks["power_plants.wind_farms.insert"]
    generate_pv_ground_mounted = tasks["power_plants.pv_ground_mounted.insert"]
    solar_rooftop_etrago = tasks["power_plants.pv_rooftop.pv-rooftop-per-mv-grid"]

    scenario_input_import >> generate_wind_farms
    hvmv_substation_extraction >> generate_wind_farms
    scenario_input_import >> generate_pv_ground_mounted
    hvmv_substation_extraction >> generate_pv_ground_mounted
    nep_insert_data >> power_plant_import
    map_boundaries_grid_districts >> solar_rooftop_etrago
    elec_cts_demands_zensus >> solar_rooftop_etrago
    elec_household_demands_zensus >> solar_rooftop_etrago
    nep_insert_data >> solar_rooftop_etrago
    etrago_input_data >> solar_rooftop_etrago
    map_zensus_grid_districts >> solar_rooftop_etrago

    # Heat supply
    heat_supply = HeatSupply(
        dependencies=[data_bundle])

    import_district_heating_supply = tasks["heat_supply.district-heating"]
    import_individual_heating_supply = tasks["heat_supply.individual-heating"]
    heat_supply_tables = tasks["heat_supply.create-tables"]
    geothermal_potential = tasks["heat_supply.geothermal.potential-germany"]

    create_district_heating_areas_table >> heat_supply_tables
    import_district_heating_areas >> import_district_heating_supply
    map_zensus_grid_districts >> import_district_heating_supply
    import_district_heating_areas >> geothermal_potential
    import_district_heating_areas >> import_individual_heating_supply
    map_zensus_grid_districts >> import_individual_heating_supply
    power_plant_import >> import_individual_heating_supply

    # Heat to eTraGo
    heat_etrago = HeatEtrago(
        dependencies=[heat_supply, mv_grid_districts])

    heat_etrago_buses = tasks["heat_etrago.buses"]
    heat_etrago_supply = tasks["heat_etrago.supply"]

    etrago_input_data >> heat_etrago_buses
