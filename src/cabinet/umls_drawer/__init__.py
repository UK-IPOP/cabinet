"""This drawer is for UMLS related activities.

The `scispacy_ner` module gives you access to the scispacy biomedical NER model
via our API.

The `metamap_ner` module interacts with the MetaMap NLP tool to extract structured
information from biomedical text and requires you to have MetaMap installed locally.

The `knowledge_base` module allows you to interact with the UMLS knowledge base data
at a high level and mostly focuses on SNOMED CT concepts. Further work on this module may take 
advantage of the *entire* UMLS, but require a locally downloaded copy due to licensing restrictions.

In general, we recommend using the `scispacy_ner` module for NER tasks and the `knowledge_base` module
for knowledge base related tasks unless you specifically need the power of MetaMap.

The `post_ner` methods exposed here utilize the API to perform NER on your text.
"""
from .scispacy_ner import post_ner_single, post_ner_many, websocket_ner, NEROutput
from .metamap_ner import MetaMap
