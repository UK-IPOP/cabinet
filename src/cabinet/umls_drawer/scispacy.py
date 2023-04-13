from pydantic import BaseModel, PrivateAttr

try:
    import spacy
    import scispacy

    # bring into scope
    from scispacy.linking import EntityLinker
    from spacy.language import Language
    from spacy.tokens import Doc

    # explicitly use a model import to ensure its available
except ImportError:
    raise ImportError(
        "It seems you are trying to use nlp_drawer without installing the required dependencies. Please install using `pip install cabinet[nlp]`"
    )


class SciSpacy(BaseModel):
    """Class for running SciSpacy on a text string."""

    _nlp: Language = PrivateAttr(default=None)
    _linker: EntityLinker = PrivateAttr(default=None)

    def load_model(self, *kwargs):
        print("Loading large sci-spacy model...")
        nlp: Language = spacy.load("en_core_sci_lg", *kwargs)
        nlp.add_pipe(
            "scispacy_linker",
            config={
                "resolve_abbreviations": True,
                "k": 30,
                "threshold": 0.9,
                "no_definition_threshold": 0.95,
                "filter_for_definitions": True,
                "max_entities_per_mention": 1,
            },
        )
        nlp.add_pipe("sentencizer")
        print("Model loaded.")
        self._nlp = nlp
        self._linker = nlp.get_pipe("scispacy_linker")

    # TODO: ideally we want these to return a custom type
    # that has simple structure (e.g. cui, name, score, entity, etc)
    def run_one(self, text: str) -> Doc:
        doc = self._nlp(text)
        return doc

    def run_many(self, texts: list[str], *kwargs) -> list[Doc]:
        docs = []
        for doc in self._nlp.pipe(texts, *kwargs):
            docs.append(doc)
        return docs


if __name__ == "__main__":
    spacy_model = SciSpacy()
    spacy_model.load_model()
    doc = spacy_model.run_one(
        "I have checmical disease because i love cocaine and heroin."
    )
    print(doc.ents)
    print(doc._.kb_ents)
