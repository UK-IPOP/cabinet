"""This module contains functions for interacting with the scispacy NER model via our API.

The private functions utilize async/await syntax and are used by the public functions which are synchronous. 
The public functions are the ones that you should use in your code unless you are confident that you know what you are doing. 

The core type of this module is `NEROutput` which is a pydantic model that represents the output from the scispacy NER model.
All public functions return either an instance of this class or an iterator of instances of this class attached to an index (tuple[int, NEROutput]) 
for the index of the text that was submitted... this helps with link to original data.

`web_socket_ner`, specifically, returns an iterator and thus needs to be consumed to be used:
```python
>>> from cabinet.umls_drawer.scispacy import web_socket_ner
>>> for text_index, ner_output in web_socket_ner(texts=["cocaine", "heroin", "cociane"]):
...     print(text_index, ner_output)
0 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"cocaine"}' score=1.0
1 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"heroin"}' score=1.0
2 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"cociane"}' score=1.0
```
"""
from __future__ import annotations

import asyncio
from typing import AsyncIterator

import aiohttp
import orjson
from pydantic import BaseModel, validate_arguments
from tqdm.asyncio import tqdm_asyncio

from cabinet.utils import _API_URL, _WS_URL


class NEROutput(BaseModel):
    """Output from the ([scispacy](https://github.com/allenai/scispacy/tree/4f9ba0931d216ddfb9a8f01334d76cfb662738ae)) NER model.

    This class is keyword only so you must pass in the arguments as: `cui="C0004096", concept_name="Acetaminophen", ...`

    Args/Attributes:
        cui (str): The UMLS CUI.
        concept_name (str): The UMLS concept name.
        concept_definition (str): The UMLS concept definition.
        entity (str): The entity that matched a UMLS concept from the source text.
        score (float): The score of the match.

    Examples:
        An example of manually creating this class:
        ```python
        >>> from cabinet.umls_drawer import NEROutput
        >>> NEROutput(
        ...     cui="C0004096",
        ...     concept_name="Acetaminophen",
        ...     concept_definition="A nonsteroidal anti-inflammatory drug that is used as an analgesic and antipyretic. It is also used in the treatment of rheumatoid arthritis and osteoarthritis.",
        ...     entity="acetaminophen",
        ...     score=0.96,
        ... )
        ```

        However, much more likely is that you get this class as a return type from one of the various functions in this module that
        make calls to our API.
        ```python
        >>> from cabinet.umls_drawer import post_ner_single
        >>> post_ner_single(text="cocaine")
        [
            0 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"cocaine"}' score=1.0
        ]
        ```

    """

    cui: str
    """The UMLS CUI."""
    concept_name: str
    """The UMLS concept name."""
    concept_definition: str
    """The UMLS concept definition."""
    entity: str
    """The entity that matched a UMLS concept from the source text."""
    score: float
    """The score of the match."""


#! this calls our api so its signature needs to match the api
async def _post_ner(session: aiohttp.ClientSession, text: str) -> list[NEROutput]:
    """Submit text to the scispacy NER model and return the results.

    Args:
        session (aiohttp.ClientSession): The session to use for the request.
        text (str): The text to submit to the NER model.

    Returns:
        list[NEROutput]: The results from the NER model.

    Raises:
        Exception: If the response status is not 200.
    """
    async with session.post("/models/ner", json={"text": text}) as resp:
        if resp.status != 200:
            raise Exception(f"Error: {resp.status}. {await resp.text()}")
        # response will have json key according to API
        data = await resp.json()
        return [NEROutput(**d) for d in data]


@validate_arguments
async def _post_nlp_single(
    text: str,
) -> list[NEROutput]:
    """Submit a single text blob to the scispacy NER model and return the results.

    Args:
        text (str): The text to submit to the NER model.

    Returns:
        list[NEROutput]: The results from the NER model.

    Raises:
        Exception: If the response status is not 200.
    """
    async with aiohttp.ClientSession(_API_URL) as session:
        return await _post_ner(session, text)


@validate_arguments
async def _post_ner_many(
    texts: list[str],
    with_progress: bool = True,
) -> list[tuple[int, NEROutput]]:
    """Submit multiple text blobs to the scispacy NER model and return the results.

    Args:
        texts (list[str]): The texts to submit to the NER model.
        with_progress (bool, optional): Whether or not to show a progress bar. Defaults to True.

    Returns:
        list[NEROutput]: The results from the NER model.

    Raises:
        Exception: If the response status is not 200.
    """
    async with aiohttp.ClientSession(_API_URL) as session:
        results: list[tuple[int, NEROutput]] = []
        tasks = set()
        for i, text in enumerate(texts):
            task = asyncio.create_task(
                _post_ner(session=session, text=text), name=f"Task {i}"
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)

            match with_progress:
                case True:
                    for task_result in tqdm_asyncio.as_completed(tasks):
                        result = await task_result
                        results.append((i, result))
                case False:
                    for task_result in asyncio.as_completed(tasks):
                        result = await task_result
                        results.append((i, result))
        return results


@validate_arguments
def post_ner_single(text: str) -> list[NEROutput]:
    """Submit a single text blob to the scispacy NER model and return the results.

    Args:
        text (str): The text to submit to the NER model.

    Returns:
        list[NEROutput]: The results from the NER model.

    Raises:
        Exception: If the response status is not 200.

    Example:
        ```python
        >>> from cabinet.umls_drawer import post_ner_single
        >>> post_ner_single(text="acetaminophen")
        [
            NEROutput(
                cui="C0004096",
                concept_name="Acetaminophen",
                concept_definition="A nonsteroidal anti-inflammatory drug that is used as an analgesic and antipyretic. It is also used in the treatment of rheumatoid arthritis and osteoarthritis.",
                entity="acetaminophen",
                score=0.96,
            )
        ]
    """
    return asyncio.run(_post_nlp_single(text))


@validate_arguments
def post_ner_many(
    texts: list[str],
    with_progress: bool = True,
) -> list[tuple[int, NEROutput]]:
    """Submit multiple text blobs to the scispacy NER model and return the results.

    Args:
        texts (list[str]): The texts to submit to the NER model.
        with_progress (bool, optional): Whether or not to show a progress bar. Defaults to True.

    Returns:
        Iterator[tuple[int, NEROutput]]: The results from the NER model.

    Raises:
        Exception: If the response status is not 200.

    Example:
        ```python
        >>> from cabinet.umls_drawer import post_ner_many
        >>> post_ner_many(texts=["acetaminophen", "ibuprofen"])
        [
            (0, NEROutput(
                cui="C0004096",
                concept_name="Acetaminophen",
                concept_definition="A nonsteroidal anti-inflammatory drug that is used as an analgesic and antipyretic. It is also used in the treatment of rheumatoid arthritis and osteoarthritis.",
                entity="acetaminophen",
                score=0.96,
            )),
            (1, NEROutput(
                cui="C0004096",
                concept_name="Ibuprofen",
                concept_definition="A nonsteroidal anti-inflammatory drug that is used as an analgesic and antipyretic. It is also used in the treatment of rheumatoid arthritis and osteoarthritis.",
                entity="ibuprofen",
                score=0.96,
            ))
        ]
    """
    return asyncio.run(_post_ner_many(texts, with_progress=with_progress))


#! this endpoint and payload name also need to match the api
@validate_arguments
async def websocket_ner(
    texts: list[str], with_progress: bool = True
) -> AsyncIterator[tuple[int, NEROutput]]:
    """Connect to the scispacy NER model websocket and submit texts.

    *IMPORTANT*: This function requires using the `async for` syntax and thus may not work in all scenarios or environments.
    It exists for **very** large datasets where the overhead of the HTTP request/response cycle is too much.

    Args:
        texts (list[str]): The texts to submit to the NER model.
        with_progress (bool, optional): Whether or not to show a progress bar. Defaults to True.

    Yields:
        AsyncIterator[tuple[int, NEROutput]]: The results from the NER model.

    Raises:
        Exception: If the response status is not 200.

    Example:
        ```python
        >>> from cabinet.umls_drawer import websocket_ner
        >>> async for i, result in websocket_ner(texts=["acetaminophen", "ibuprofen"]):
        ...     print(i, result)
        0 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"cocaine"}' score=1.0
        1 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"heroin"}' score=1.0
        2 cui='12' concept_name='test' concept_definition='test22' entity='{"text":"cociane"}' score=1.0
    """
    async with aiohttp.ClientSession(_WS_URL) as session:
        async with session.ws_connect("/models/ner/ws") as ws:
            match with_progress:
                case True:
                    for i, text in tqdm_asyncio(enumerate(texts)):
                        await ws.send_bytes(orjson.dumps({"text": text}))
                        raw_data = await ws.receive_bytes()
                        response_data = orjson.loads(raw_data)
                        data = NEROutput(**response_data)
                        yield (i, data)
                case False:
                    for i, text in enumerate(texts):
                        await ws.send_bytes(orjson.dumps({"text": text}))
                        raw_data = await ws.receive_bytes()
                        response_data = orjson.loads(raw_data)
                        data = NEROutput(**response_data)
                        yield (i, data)
