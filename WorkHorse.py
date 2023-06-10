import geopandas as gpd
from database_wrapper import send_to_postgis, basic_read, process_records
from typing import Optional
from fastapi.exceptions import HTTPException
import asyncio


class WorkHorse:

    def __init__(
            self,
            x_tile: int,
            y_tile: int,
            z_zoom_level: int
    ):
        self.x_tile: int = x_tile
        self.y_tile: int = y_tile
        self.z_zoom_level: int = z_zoom_level

        self.x_min: Optional[float] = None
        self.x_max: Optional[float] = None
        self.y_min: Optional[float] = None
        self.y_max: Optional[float] = None

    def check_if_tile_numbers_are_valid(self):
        """
        Purpose of this function is to check if tiles that
        are provided are valid.

        There is rule/formula that helps us to do that.

        """

        if 0 <= self.x_tile < 2 ** self.z_zoom_level:
            pass
        else:
            raise HTTPException(status_code=400, detail='X coordinate is problematic!!!')

        if 0 <= self.y_tile < 2 ** self.z_zoom_level:
            pass
        else:
            raise HTTPException(status_code=400, detail='Y coordinate is problematic!!!')

    def tile_to_envelope(self):
        """
        Purpose of this function is to calculate bounding box i.e.
        to create x_min, x_max, y_min, y_max coordinates.

        """

        # Width of world in EPSG:3857
        world_mercator_max = 20037508.3427892
        world_mercator_min = -1 * world_mercator_max
        world_mercator_size = world_mercator_max - world_mercator_min

        # Width in tiles
        world_tile_size = 2 ** self.z_zoom_level

        # Tile width in EPSG:3857
        tile_mercator_size = world_mercator_size / world_tile_size

        # Calculate geographic bounds from tile coordinates
        # XYZ tile coordinates are in "image space" so origin is
        # top-left, not bottom right.

        self.x_min = world_mercator_min + tile_mercator_size * self.x_tile
        self.x_max = world_mercator_min + tile_mercator_size * (self.x_tile + 1)
        self.y_min = world_mercator_max - tile_mercator_size * (self.y_tile + 1)
        self.y_max = world_mercator_max - tile_mercator_size * self.y_tile

    def _envelope_to_bounds_sql(self):
        """

        Purpose of this function is to create SQL query
        that will later on be used to sub-select features
        by its geo data.

        :return:
        """

        DENSIFY_FACTOR = 4

        seg_size = (self.x_max - self.x_min) / DENSIFY_FACTOR

        return f'ST_Segmentize(ST_MakeEnvelope({self.x_min}, {self.y_min}, {self.x_max}, {self.y_max}, 3857),{seg_size})'

    def envelope_to_sql(self):
        """

        Purpose of this function is to create SQL query that
        will pull data from table and create MVT tiles.

        :return:
        """

        env_ = self._envelope_to_bounds_sql()

        sql_tmpl = f"""
            WITH 

                table_with_3857_geometry as (
                select *, st_transform(geometry, 3857) as geom_3857
                -- from test_table__county_regions
                from sd_buildings
                
            ),

            bounds AS (
                SELECT {env_} AS geom, 
                       {env_}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT ST_AsMVTGeom(ST_Transform(t.geometry, 3857), bounds.b2d) AS geom
                   -- ,t."NAME",
                   -- t."AREA_CODE",
                   -- t."DESCRIPT0",
                   -- t."FILE_NAME",
                   -- t."HECTARES"
                FROM table_with_3857_geometry t, bounds
                WHERE ST_Intersects(t.geom_3857, ST_Transform(bounds.geom, 3857))
            ) 
            SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom
        """

        return sql_tmpl

