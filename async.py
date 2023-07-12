import asyncio
import aiohttp
import datetime
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople

CHUNK_SIZE = 10


async def get_people(session, people_id):
    print(f'{people_id=} started')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        if response.status == 200:
            print(f'{people_id=} finished')
            json_data['id_people'] = str(people_id)
            films = []
            for i in json_data['films']:
                async with session.get(i) as response:
                    json_data_1 = await response.json()
                    films.append(json_data_1["title"])
            json_data['films_1'] = films
            species = []
            for i in json_data['species']:
                async with session.get(i) as response:
                    json_data_1 = await response.json()
                    species.append(json_data_1["name"])
            json_data['species_1'] = species
            starships = []
            for i in json_data['starships']:
                async with session.get(i) as response:
                    json_data_1 = await response.json()
                    starships.append(json_data_1["name"])
            json_data['starships_1'] = starships
            vehicles = []
            for i in json_data['vehicles']:
                async with session.get(i) as response:
                    json_data_1 = await response.json()
                    vehicles.append(json_data_1["name"])
            json_data['vehicles_1'] = vehicles
            async with session.get(json_data['homeworld']) as response:
                json_data_1 = await response.json()
            json_data['homeworld_1'] = json_data_1["name"]
        return json_data


async def paste_to_db(results):
    swapi_people = [SwapiPeople(
        eye_color=item['eye_color'],
        birth_year=item['birth_year'],
        films=", ".join(item['films_1']),
        species=", ".join(item['species_1']),
        starships=", ".join(item['starships_1']),
        vehicles=", ".join(item['vehicles_1']),
        gender=item['gender'],
        hair_color=item['hair_color'],
        height=item['height'],
        mass=item['mass'],
        name=item['name'],
        skin_color=item['skin_color'],
        homeworld=item['homeworld_1'],
        id_people=item['id_people']
    ) for item in results]
    async with Session() as session:
        session.add_all(swapi_people)
        await session.commit()


async def main():
    start = datetime.datetime.now()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session = aiohttp.ClientSession()
    coros = (get_people(session, i) for i in range(1, 84))
    for coros_chunk in chunked(coros, CHUNK_SIZE):
        results = await asyncio.gather(*coros_chunk)
        if {'detail': 'Not found'} in results:
            results.remove({'detail': 'Not found'})
        asyncio.create_task(paste_to_db(results))
    await session.close()
    set_tasks = asyncio.all_tasks()
    for task in set_tasks:
        if task != asyncio.current_task():
            await task
    print(datetime.datetime.now() - start)


asyncio.run(main())