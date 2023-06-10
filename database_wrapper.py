
import geopandas as gpd
from database_connections import engine
from typing import Optional
from sqlalchemy import text
import asyncpg


def read_from_postgis(
        sql_query: str,
        parameters: Optional[dict] = None,
        geometry_column: str = 'geom'
) -> gpd.GeoDataFrame:
    with engine.connect() as con_:

        gdf: gpd.GeoDataFrame = gpd.read_postgis(
            sql=sql_query,
            con=con_,
            geom_col=geometry_column,
            params=parameters,
            crs=None,
            index_col=None,
            coerce_float=True,
            parse_dates=None,
            chunksize=None
        )

        return gdf


def send_to_postgis(
        table_name: str,
        gdf: gpd.GeoDataFrame,
        schema: str = 'public'
) -> None:
    with engine.connect() as con_:
        gdf.to_postgis(
            name=table_name,
            con=con_,
            schema=schema,
            if_exists='append',
            index=False,
            index_label=None,
            chunksize=None,
            dtype=None
        )


def basic_read(
        sql_query: str,
        parameters: Optional[dict] = None
):

    list_with_results = []

    with engine.connect() as conn_:

        result = conn_.execute(text(sql_query), parameters=parameters)

        for row in result:
            list_with_results.append(row)

    return list_with_results


async def process_records(
        sql_query: str
):
    conn: asyncpg.connection.Connection = await asyncpg.connect(
        host='127.0.0.1',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres'
    )
    try:
        async with conn.transaction():

            rows = await conn.fetch(sql_query)
            results = []
            for row in rows:
                results.append(row)
    finally:
        await conn.close()
    return results





