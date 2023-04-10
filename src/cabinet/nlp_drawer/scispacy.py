from dataclasses import dataclass
import warnings
import spacy
from spacy.language import Language
# explicitly use a model import to ensure its available
import en_core_sci_lg

# try:
#     from spacy.language import Language
#     import en_core_sci_lg
# except ImportError:
#     warnings.warn("It seems you are trying to use nlp_drawer without installing the required dependencies. Please install using `pip install cabinet[nlp]`", category=ImportWarning)
#     _spacy = False
#     _en_core_sci_lg = False
# else:
#     _spacy = True
#     _en_core_sci_lg = True


# def requires_scispacy(func):
#     def wrapper(*args, **kwargs):
#         if _spacy is False:
#             print("scispacy not installed. Please install using the 'nlp' extra: `pip install cabinet[nlp]`")
#             return None
#         elif _en_core_sci_lg is False:
#             print("scispacy model not installed. Please install using the 'nlp' extra: `pip install cabinet[nlp]`")
#             return None
#         else:
#             return func(*args, **kwargs)
#     return wrapper

# @requires_scispacy

@dataclass
class NLP_Runner:
    nlp: Language

    def load_model(self, *kwargs):
        print("Loading large sci-spacy model...")
        nlp: Language = spacy.load("en_core_sci_lg", *kwargs)
        print("Model loaded.")
        self.nlp = nlp

    def run(self, text: str):
        print("Running SciSpacy...")
        doc = self.nlp(text)
        return doc



def run():
    print("Running SciSpacy...")
