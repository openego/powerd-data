"""The central module containing code to create CH4 and H2 voronoi polygones

"""
from geoalchemy2.types import Geometry
from geovoronoi import voronoi_regions_from_coords
import geopandas as gpd


def get_voronoi_geodataframe(buses, boundary):
    """
    Create voronoi polygons for the passed buses within the boundaries.

    Parameters
    ----------
    buses : geopandas.GeoDataFrame
        Buses to create the voronois for.

    boundary : Multipolygon, Polygon
        Bounding box for the voronoi generation.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containting the bus_ids and the respective voronoi
        polygons.

    """
    buses = buses[buses.geometry.intersects(boundary)]

    coords = buses[["x", "y"]].values  # coordinates of the respective buses

    region_polys, region_pts = voronoi_regions_from_coords(coords, boundary)

    gpd_input_dict = {
        "bus_id": [],  # original bus_id in the buses dataframe
        "geometry": [],  # voronoi object
    }

    for pt, poly in region_pts.items():
        gpd_input_dict["geometry"] += [region_polys[pt]]
        gpd_input_dict["bus_id"] += [buses.iloc[poly[0]]["bus_id"]]

    return gpd.GeoDataFrame(gpd_input_dict)
