
import geopandas as gpd
from database_wrapper import send_to_postgis

gdf = gpd.read_file('data/vector_map_district/OS VectorMap District (ESRI Shape File) SD/data/SD_Building.shp')

send_to_postgis(
    table_name='sd_buildings',
    gdf=gdf
)

