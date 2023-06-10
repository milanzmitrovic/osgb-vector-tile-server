
# https://www.crunchydata.com/blog/dynamic-vector-tiles-from-postgis
# https://security.openstack.org/guidelines/dg_parameterize-database-queries.html

from fastapi import FastAPI
import uvicorn
from starlette.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from WorkHorse import WorkHorse
import datetime
from database_wrapper import process_records, basic_read


app = FastAPI()


origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://localhost:63342",

]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get(path='/{z}/{x}/{y}.mvt')
async def get_vector_tiles(z: int, x: int, y: int):

    # print('Start sleeping...')
    # # await asyncio.sleep(3)
    # print('Waking up...')

    wh = WorkHorse(x_tile=x, y_tile=y, z_zoom_level=z)

    wh.check_if_tile_numbers_are_valid()

    wh.tile_to_envelope()

    sql_query_ = wh.envelope_to_sql()

    # --- --- --- --- --- #
    # THIS IS VARIANT FOR SYNCHRONOUS QUERY EXECUTION

    # print(f"Before executing SQL query...{z}/{x}/{y}...{datetime.datetime.now()}")
    # response = Response(bytes(basic_read(
    #     sql_query=sql_query_
    # )[0][0]))
    # print(f"After executing SQL query...{z}/{x}/{y}...{datetime.datetime.now()}")
    #
    # return response

    # END OF BLOCK
    # --- --- --- --- --- #

    # --- --- --- --- --- #
    # THIS IS VARIANT FOR ASYNCHRONOUS QUERY EXECUTION

    print(f"Before executing SQL query...{z}/{x}/{y}...{datetime.datetime.now()}")
    pbf_data = await process_records(
        sql_query=sql_query_
    )
    print(f"After executing SQL query...{z}/{x}/{y}...{datetime.datetime.now()}")

    return Response(
        pbf_data[0]['st_asmvt']
    )

    # END OF BLOCK
    # --- --- --- --- --- #


if __name__ == '__main__':
    uvicorn.run(app=app, workers=1)




