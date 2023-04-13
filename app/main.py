import json
import aiohttp
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.responses import RedirectResponse
import requests

app = FastAPI()


@app.get("/", response_class=ORJSONResponse)
def read_root():
    return {"Hello": "World"}


@app.post("/nlp", response_class=ORJSONResponse)
async def post_nlp(text: str):
    return {"text": text.upper()}


@app.get("/releases/{release_name}", response_class=RedirectResponse)
def get_release(release_name: str):
    with open("data/open-data-releases.json", "r") as f:
        data = json.load(f)
    return data[1]["url"]
