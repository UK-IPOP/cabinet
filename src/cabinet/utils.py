from rich import pretty
from rich.console import Console
import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio

pretty.install()

console = Console()


async def post_nlp(session: aiohttp.ClientSession, text: str)
    async with session.post("/nlp", json={"text": text}) as resp:
        return await resp.json()

async def get_pokemon(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        return await resp.json()


async def fetch_one_pokemon():
    async with aiohttp.ClientSession("https://pokeapi.co") as session:
        async with session.get("api/v2/pokemon/6") as resp:
            return await resp.json()


def return_pokemon():
    return asyncio.run(fetch_one_pokemon())


async def fetch_many_pokemon(with_progress: bool):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = set()
        for i in range(1, 10):
            url = f"https://pokeapi.co/api/v2/pokemon/{i}"
            task = asyncio.create_task(get_pokemon(session, url))
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        if with_progress:
            for task_result in tqdm_asyncio.as_completed(tasks):
                result = await task_result
                results.append(result)
                console.log(result["name"], result["id"], style="bold green")
        else:
            for task_result in asyncio.as_completed(tasks):
                result = await task_result
                results.append(result)
                console.log(result["name"], result["id"], style="bold green")
    return results


def return_many_pokemon(with_progress: bool = True):
    return asyncio.run(fetch_many_pokemon(with_progress=with_progress))
