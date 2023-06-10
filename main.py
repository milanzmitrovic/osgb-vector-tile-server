
# https://www.crunchydata.com/blog/dynamic-vector-tiles-from-postgis
# https://security.openstack.org/guidelines/dg_parameterize-database-queries.html

from fastapi import FastAPI
import uvicorn
from time import sleep
from starlette.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from WorkHorse import WorkHorse
import asyncio
import datetime


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
    print(f"Before executing SQL query...{z}/{x}/{y}...{datetime.datetime.now()}")
    pbf_data = await wh.sql_to_pbf()
    print(f"After executing SQL query...{z}/{x}/{y}...{datetime.datetime.now()}")
    # return Response(bytes(wh.sql_to_pbf()[0][0]))
    return Response(
        pbf_data[0]['st_asmvt']
    )


if __name__ == '__main__':
    uvicorn.run(app=app, workers=1)




