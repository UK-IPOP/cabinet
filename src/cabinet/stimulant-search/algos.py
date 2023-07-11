import argparse
from pathlib import Path
from typing import Iterable, Literal
import orjson
import polars as pl
import pandas as pd
from pydantic import BaseModel, root_validator
import time
import warnings
from rich import print, print_json
from rich.console import Console
import exrex


_CDC_WARNING_TEXT = """
Tracing is resource intensive and will consume significantly 
more memory than the untraced query. It will also 
take ~5x longer than untraced queries. Use with caution.
"""

_BETTANO_WARNING_TEXT = """
Tracing is resource intensive and will consume significantly 
more memory than the untraced query. It will also take ~XXXx
longer than untraced queries. Use with caution.
"""


# little hacky hack for pydantic and orjson
def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


def to_lazy(df: pl.DataFrame | pl.LazyFrame | pd.DataFrame) -> pl.LazyFrame:
    if isinstance(df, pl.DataFrame):
        return df.lazy()
    elif isinstance(df, pd.DataFrame):
        return pl.from_pandas(df).lazy()
    elif isinstance(df, pl.LazyFrame):
        return df
    else:
        raise TypeError("table must be polars DataFrame/LazyFrame or pandas DataFrame")


def make_regex(terms: Iterable[str]) -> str:
    return exrex.simplify(r"|".join(terms))


def expand_regex(regex: str) -> set[str]:
    return set(exrex.generate(regex))


def _check_col(col: str, df: pl.DataFrame | pl.LazyFrame | pd.DataFrame) -> None:
    if col not in df.columns:
        raise ValueError(f"{col} not in DataFrame")


class CDCPatterns(BaseModel):
    codes: list[str]
    inclusion1: list[str]
    inclusion2: list[str]
    exclusion: list[str]
    crack: str
    crack_pairs: list[str]
    rum: str
    coke: str

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    def show_patterns(self) -> None:
        print_json(
            data={
                "codes": self.codes,
                "inclusion1": self.inclusion1,
                "inclusion2": self.inclusion2,
                "exclusion": self.exclusion,
                "crack": self.crack,
                "crack_pairs": self.crack_pairs,
                "rum": self.rum,
                "coke": self.coke,
            }
        )


class CDCOptions(BaseModel):
    text_col: str
    tracing: bool = False
    _patterns: CDCPatterns = CDCPatterns.parse_file("cdc_patterns.json")

    class Config:
        underscore_attrs_are_private = True

    def codes_regex(self) -> str:
        codes: list[str] = []
        for code in self._patterns.codes:
            # remove dots as defined in the CDC algorithm
            codes.append(code.replace(r".", r"\."))
            codes.append(code.replace(r".", ""))
        return make_regex(codes)

    def inclusion1_regex(self) -> str:
        return make_regex(self._patterns.inclusion1)

    def inclusion2_regex(self) -> str:
        return make_regex(self._patterns.inclusion2)

    def exclusion_regex(self) -> str:
        return make_regex(self._patterns.exclusion)

    def codes_strings(self) -> set[str]:
        return expand_regex(self.codes_regex())

    def inclusion1_strings(self) -> set[str]:
        return expand_regex(self.inclusion1_regex())

    def inclusion2_strings(self) -> set[str]:
        return expand_regex(self.inclusion2_regex())

    def exclusion_strings(self) -> set[str]:
        return expand_regex(self.exclusion_regex())

    def regex_steps(self) -> list[tuple[str, str]]:
        return [
            ("codes", self.codes_regex()),
            ("inclusion1", self.inclusion1_regex()),
            ("inclusion2", self.inclusion2_regex()),
            ("exclusion", self.exclusion_regex()),
            ("crack_pairs", make_regex(self._patterns.crack_pairs)),
            ("rum", self._patterns.rum),
            ("coke", self._patterns.coke),
            ("crack", self._patterns.crack),
        ]

    def all_terms(self) -> set[str]:
        unique_terms: set[str] = set()
        unique_terms.update(self.codes_strings())
        unique_terms.update(self.inclusion1_strings())
        unique_terms.update(self.inclusion2_strings())
        unique_terms.update(self.exclusion_strings())
        unique_terms.update(set(self._patterns.crack_pairs))
        unique_terms.add(self._patterns.crack)
        unique_terms.add(self._patterns.rum)
        unique_terms.add(self._patterns.coke)
        return unique_terms


def _cdc_untraced_query(df: pl.LazyFrame, config: CDCOptions) -> pl.LazyFrame:
    # case insensitive hack: https://pola-rs.github.io/polars/py-polars/html/reference/series/api/polars.Series.str.contains.html
    for name, pattern in config.regex_steps():
        df = df.with_columns(
            [
                pl.col(config.text_col)
                .str.contains(
                    pattern=rf"(?i){pattern}",
                    literal=False,
                )
                .alias(name),
            ]
        )
    df = df.with_columns(
        [
            ((pl.col("crack") > 0) & (pl.any(pl.col("crack_pairs") > 0))).alias(
                "crack_exclude"
            ),
            ((pl.col("rum") > 0) & (pl.col("coke") > 0)).alias("rum_coke"),
        ]
    )
    return df


def _cdc_traced_query(df: pl.LazyFrame, config: CDCOptions) -> pl.LazyFrame:
    df = df.with_columns(
        [
            pl.col(config.text_col).str.count_match(rf"(?i){term}").alias(term)
            for term in config.all_terms()
        ]
    )
    df = df.with_columns(
        [
            pl.any(pl.col(config.codes_strings()) > 0).alias("codes"),
            pl.any(pl.col(config.inclusion1_strings()) > 0).alias("inclusion1"),
            pl.any(pl.col(config.inclusion2_strings()) > 0).alias("inclusion2"),
            pl.any(pl.col(config.exclusion_strings()) > 0).alias("exclusion"),
            (
                (pl.col(config._patterns.crack) > 0)
                & (pl.any(pl.col(config._patterns.crack_pairs) > 0))
            ).alias("crack_exclude"),
            (
                (pl.col(config._patterns.rum) > 0) & (pl.col(config._patterns.coke) > 0)
            ).alias("rum_coke"),
        ]
    )
    return df


def _resolve_cdc(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        [
            pl.when(pl.col("codes"))
            .then(True)
            .when(
                pl.col("inclusion1")
                & pl.col("inclusion2")
                & pl.col("exclusion").is_not()
                & pl.col("crack_exclude").is_not()
                & pl.col("rum_coke").is_not()
            )
            .then(True)
            .otherwise(False)
            .alias("signal")
        ]
    )


def search_cdc(
    table: pl.DataFrame | pl.LazyFrame | pd.DataFrame,
    search_col: str,
    tracing: bool = False,
) -> pl.LazyFrame:
    config = CDCOptions(text_col=search_col, tracing=tracing)
    _check_col(col=config.text_col, df=table)
    lazy_df = to_lazy(table)
    match config.tracing:
        case False:
            lazy_df = (
                lazy_df.pipe(_cdc_untraced_query, config=config)
                .pipe(_resolve_cdc)
                .drop(
                    columns=[
                        "codes",
                        "inclusion1",
                        "inclusion2",
                        "exclusion",
                        "crack_exclude",
                        "rum_coke",
                        "rum",
                        "coke",
                        "crack",
                        "crack_pairs",
                    ]
                )
            )
        case True:
            warnings.warn(RuntimeWarning(_CDC_WARNING_TEXT), stacklevel=3)
            lazy_df = lazy_df.pipe(_cdc_traced_query, config=config).pipe(_resolve_cdc)
    return lazy_df


class SimpleCriteria(BaseModel):
    inclusion: list[str]
    exclusion: list[str] | None = None

    def inclusion_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion])

    def exclusion_regex(self) -> str | None:
        if self.exclusion is None:
            return None
        return make_regex([item.replace(r".", r"\.") for item in self.exclusion])

    def inclusion_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex())

    def exclusion_strings(self) -> set[str] | None:
        # we have to explicitly assign variable here to satisfy linters
        ex_re = self.exclusion_regex()
        if ex_re is None:
            return None
        return expand_regex(ex_re)


class Level1(BaseModel):
    criteria1: SimpleCriteria
    criteria2: SimpleCriteria
    criteria3: SimpleCriteria
    criteria4: SimpleCriteria
    criteria5: SimpleCriteria
    criteria6: SimpleCriteria

    class Config:
        arbitrary_types_allowed = True

    def criteria_list(self) -> list[SimpleCriteria]:
        return [
            self.criteria1,
            self.criteria2,
            self.criteria3,
            self.criteria4,
            self.criteria5,
            self.criteria6,
        ]


class DoubleNegativeCriteria(BaseModel):
    inclusion: list[str]
    exclusion1: list[str]
    exclusion2: list[str]

    def inclusion_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion])

    def exclusion1_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.exclusion1])

    def exclusion2_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.exclusion2])

    def inclusion_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex())

    def exclusion1_strings(self) -> set[str]:
        return expand_regex(self.exclusion1_regex())

    def exclusion2_strings(self) -> set[str]:
        return expand_regex(self.exclusion2_regex())


class DoublePositiveCriteria(BaseModel):
    inclusion1: list[str]
    inclusion2: list[str]
    exclusion: list[str]

    def inclusion1_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion1])

    def inclusion2_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion2])

    def exclusion_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.exclusion])

    def inclusion1_strings(self) -> set[str]:
        return expand_regex(self.inclusion1_regex())

    def inclusion2_strings(self) -> set[str]:
        return expand_regex(self.inclusion2_regex())

    def exclusion_strings(self) -> set[str]:
        return expand_regex(self.exclusion_regex())


class TripleNegativeCriteria(BaseModel):
    inclusion: list[str]
    exclusion1: list[str] | None = None
    exclusion2: list[str] | None = None
    exclusion3: list[str] | None = None

    def inclusion_regex(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion])

    def exclusion1_regex(self) -> str | None:
        if self.exclusion1 is None:
            return None
        return make_regex(self.exclusion1)

    def exclusion2_regex(self) -> str | None:
        if self.exclusion2 is None:
            return None
        return make_regex(self.exclusion2)

    def exclusion3_regex(self) -> str | None:
        if self.exclusion3 is None:
            return None
        return make_regex(self.exclusion3)

    def inclusion_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex())

    def exclusion1_strings(self) -> set[str] | None:
        ex_re = self.exclusion1_regex()
        if ex_re is None:
            return None
        return expand_regex(ex_re)

    def exclusion2_strings(self) -> set[str] | None:
        ex_re = self.exclusion2_regex()
        if ex_re is None:
            return None
        return expand_regex(ex_re)

    def exclusion3_strings(self) -> set[str] | None:
        ex_re = self.exclusion3_regex()
        if ex_re is None:
            return None
        return expand_regex(ex_re)


class Criteria5ListB(BaseModel):
    needle: TripleNegativeCriteria
    syringe: TripleNegativeCriteria
    shotup: TripleNegativeCriteria
    presc: TripleNegativeCriteria
    snort: TripleNegativeCriteria
    inject: TripleNegativeCriteria
    crush: TripleNegativeCriteria
    misuse: TripleNegativeCriteria
    recreational: TripleNegativeCriteria

    class Config:
        arbitrary_types_allowed = True


class Criteria5(BaseModel):
    lista: list[str]
    listb: Criteria5ListB
    listc: list[str]

    class Config:
        arbitrary_types_allowed = True

    def all_words(self) -> set[str]:
        terms = set()
        terms.update(self.lista)
        terms.update(self.listc)
        terms.update(self.listb.needle.inclusion)
        terms.update(self.listb.syringe.inclusion)
        terms.update(self.listb.shotup.inclusion)
        terms.update(self.listb.presc.inclusion)
        terms.update(self.listb.snort.inclusion)
        terms.update(self.listb.inject.inclusion)
        terms.update(self.listb.crush.inclusion)
        terms.update(self.listb.misuse.inclusion)
        terms.update(self.listb.recreational.inclusion)
        terms.update(self.listb.needle.exclusion1 or [])
        terms.update(self.listb.syringe.exclusion1 or [])
        terms.update(self.listb.shotup.exclusion1 or [])
        terms.update(self.listb.presc.exclusion1 or [])
        terms.update(self.listb.snort.exclusion1 or [])
        terms.update(self.listb.inject.exclusion1 or [])
        terms.update(self.listb.crush.exclusion1 or [])
        terms.update(self.listb.misuse.exclusion1 or [])
        terms.update(self.listb.recreational.exclusion1 or [])
        terms.update(self.listb.needle.exclusion2 or [])
        terms.update(self.listb.syringe.exclusion2 or [])
        terms.update(self.listb.shotup.exclusion2 or [])
        terms.update(self.listb.presc.exclusion2 or [])
        terms.update(self.listb.snort.exclusion2 or [])
        terms.update(self.listb.inject.exclusion2 or [])
        terms.update(self.listb.crush.exclusion2 or [])
        terms.update(self.listb.misuse.exclusion2 or [])
        terms.update(self.listb.recreational.exclusion2 or [])
        terms.update(self.listb.needle.exclusion3 or [])
        terms.update(self.listb.syringe.exclusion3 or [])
        terms.update(self.listb.shotup.exclusion3 or [])
        terms.update(self.listb.presc.exclusion3 or [])
        terms.update(self.listb.snort.exclusion3 or [])
        terms.update(self.listb.inject.exclusion3 or [])
        terms.update(self.listb.crush.exclusion3 or [])
        terms.update(self.listb.misuse.exclusion3 or [])
        terms.update(self.listb.recreational.exclusion3 or [])
        regex = make_regex([item.replace(r".", r"\.") for item in terms])
        return expand_regex(regex)


class Level2(BaseModel):
    criteria1: SimpleCriteria
    criteria2: SimpleCriteria
    criteria3: SimpleCriteria
    criteria4: SimpleCriteria
    criteria5: Criteria5
    criteria6: DoublePositiveCriteria
    criteria7: SimpleCriteria

    class Config:
        arbitrary_types_allowed = True

    def criteria_list(
        self,
    ) -> list[
        SimpleCriteria | DoubleNegativeCriteria | DoublePositiveCriteria | Criteria5
    ]:
        return [
            self.criteria1,
            self.criteria2,
            self.criteria3,
            self.criteria4,
            self.criteria5,
            self.criteria6,
            self.criteria7,
        ]


class TwoPartCriteria(BaseModel):
    inclusion1: list[str]
    inclusion2: list[str]
    exclusion1: list[str]
    exclusion2: list[str]

    def inclusion_regex1(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion1])

    def inclusion_regex2(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion2])

    def exclusion_regex1(self) -> str:
        return make_regex(self.exclusion1)

    def exclusion_regex2(self) -> str:
        return make_regex(self.exclusion2)

    def inclusion1_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex1())

    def inclusion2_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex2())

    def exclusion1_strings(self) -> set[str]:
        return expand_regex(self.exclusion_regex1())

    def exclusion2_strings(self) -> set[str]:
        return expand_regex(self.exclusion_regex2())


class TriplePositiveCriteria(BaseModel):
    inclusion1: list[str]
    inclusion2: list[str]
    inclusion3: list[str]

    def inclusion_regex1(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion1])

    def inclusion_regex2(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion2])

    def inclusion_regex3(self) -> str:
        return make_regex([item.replace(r".", r"\.") for item in self.inclusion3])

    def inclusion1_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex1())

    def inclusion2_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex2())

    def inclusion3_strings(self) -> set[str]:
        return expand_regex(self.inclusion_regex3())


class Level3(BaseModel):
    criteria1: SimpleCriteria
    criteria2: SimpleCriteria
    criteria3: SimpleCriteria
    criteria4: SimpleCriteria
    criteria5: SimpleCriteria
    criteria6: TwoPartCriteria
    criteria7: SimpleCriteria
    criteria8: SimpleCriteria
    criteria9: SimpleCriteria
    criteria10: SimpleCriteria
    criteria11: TriplePositiveCriteria
    criteria12: SimpleCriteria
    criteria13: SimpleCriteria

    class Config:
        arbitrary_types_allowed = True

    def criteria_list(
        self,
    ) -> list[SimpleCriteria | TwoPartCriteria | TriplePositiveCriteria]:
        return [
            self.criteria1,
            self.criteria2,
            self.criteria3,
            self.criteria4,
            self.criteria5,
            self.criteria6,
            self.criteria7,
            self.criteria8,
            self.criteria9,
            self.criteria10,
            self.criteria11,
            self.criteria12,
            self.criteria13,
        ]


class BettanoPatterns(BaseModel):
    level1: Level1
    level2: Level2
    level3: Level3

    class Config:
        arbitrary_types_allowed = True

    def levels(self, depth: int) -> list[Level1 | Level2 | Level3]:
        levels: list[Level1 | Level2 | Level3] = [self.level1, self.level2, self.level3]
        return levels[:depth]

    def get_combined_strings(self, depth: int) -> set[str]:
        terms: set[str] = set()
        for level in self.levels(depth=depth):
            if isinstance(level, Level1):
                for criteria in level.criteria_list():
                    terms |= criteria.inclusion_strings()
                    exc_strings = criteria.exclusion_strings()
                    if exc_strings is not None:
                        terms |= exc_strings
            elif isinstance(level, Level2):
                for criteria in level.criteria_list():
                    if isinstance(criteria, SimpleCriteria):
                        terms |= criteria.inclusion_strings()
                        exc_strings = criteria.exclusion_strings()
                        if exc_strings is not None:
                            terms |= exc_strings
                    elif isinstance(criteria, DoubleNegativeCriteria):
                        terms |= criteria.inclusion_strings()
                        exc1_strings = criteria.exclusion1_strings()
                        exc2_strings = criteria.exclusion2_strings()
                        if exc1_strings is not None:
                            terms |= exc1_strings
                        if exc2_strings is not None:
                            terms |= exc2_strings
                    elif isinstance(criteria, DoublePositiveCriteria):
                        terms |= criteria.inclusion1_strings()
                        terms |= criteria.inclusion2_strings()
                        exc_strings = criteria.exclusion_strings()
                        if exc_strings is not None:
                            terms |= exc_strings
                    elif isinstance(criteria, Criteria5):
                        terms |= criteria.all_words()
                    else:
                        raise ValueError(
                            f"Unknown criteria type for lvl 2: {type(criteria)}"
                        )
            elif isinstance(level, Level3):
                for criteria in level.criteria_list():
                    if isinstance(criteria, SimpleCriteria):
                        terms |= criteria.inclusion_strings()
                        exc_strings = criteria.exclusion_strings()
                        if exc_strings is not None:
                            terms |= exc_strings
                    elif isinstance(criteria, TwoPartCriteria):
                        terms |= criteria.inclusion1_strings()
                        terms |= criteria.inclusion2_strings()
                        terms |= criteria.exclusion1_strings()
                        terms |= criteria.exclusion2_strings()
                    elif isinstance(criteria, TriplePositiveCriteria):
                        terms |= criteria.inclusion1_strings()
                        terms |= criteria.inclusion2_strings()
                        terms |= criteria.inclusion3_strings()
                    else:
                        raise ValueError(
                            f"Unknown criteria type for lvl 3: {type(criteria)}"
                        )
            else:
                raise ValueError(f"Unknown level type: {type(level)}")
        return terms

    def show_patterns(self) -> None:
        print_json(
            data={
                "level1": self.level1.dict(),
                "level2": self.level2.dict(),
                "level3": self.level3.dict(),
            }
        )


class BettanoOptions(BaseModel):
    text_col: str
    age_col: str | None
    tracing: bool = False
    depth: Literal[1, 2, 3]
    _patterns: BettanoPatterns = BettanoPatterns.parse_file("bettano_patterns.json")

    class Config:
        underscore_attrs_are_private = True
        arbitrary_types_allowed = True

    @root_validator(skip_on_failure=True)
    def check_age_col(cls, values):
        if values["depth"] > 1 and values["age_col"] is None:
            raise ValueError("age_col must be provided when depth > 1")
        if values["depth"] == 1 and values["age_col"] is not None:
            warnings.warn(
                UserWarning(
                    "age_col is not used when depth == 1. Ignoring provided age column..."
                ),
                stacklevel=3,
            )
        return values


def _add_age_columns(df: pl.LazyFrame, age_col: str) -> pl.LazyFrame:
    return df.with_columns(
        [
            (pl.col(age_col) > 14).alias(">14"),
            (pl.col(age_col) < 46).alias("<46"),
            (pl.col(age_col) < 55).alias("<55"),
        ]
    )


def _tag_simple_criteria(
    df: pl.LazyFrame, crit: int, lvl: int, criteria: SimpleCriteria
) -> pl.LazyFrame:
    inc_cols = criteria.inclusion_strings()
    exc_cols = criteria.exclusion_strings()
    if exc_cols is None:
        df = df.with_columns(
            [pl.any(pl.col(inc_cols) > 0).alias(f"lvl{lvl}_crit{crit}")]
        )
    else:
        df = df.with_columns(
            [
                (
                    pl.any(pl.col(inc_cols) > 0) & pl.any(pl.col(exc_cols) > 0).is_not()
                ).alias(f"lvl{lvl}_crit{crit}"),
            ]
        )
    return df


def _aggregate_level1(df: pl.LazyFrame, c: BettanoOptions) -> pl.LazyFrame:
    criteria_list = c._patterns.level1.criteria_list()
    for i, criteria in enumerate(criteria_list, start=1):
        df = df.pipe(_tag_simple_criteria, crit=i, lvl=1, criteria=criteria)
    df = df.with_columns([pl.any(pl.col(r"^lvl1_crit\d$")).alias("lvl1")])
    return df


def _handle_criteria5(df: pl.LazyFrame, crit: Criteria5) -> pl.LazyFrame:
    lista_cols = crit.lista
    listc_cols = crit.listc
    needle_inc_cols = crit.listb.needle.inclusion_strings()
    needle_exc1_cols = crit.listb.needle.exclusion1_strings()
    needle_exc2_cols = crit.listb.needle.exclusion2_strings()
    needle_exc3_cols = crit.listb.needle.exclusion3_strings()
    syringe_inc_cols = crit.listb.syringe.inclusion_strings()
    syringe_exc1_cols = crit.listb.syringe.exclusion1_strings()
    syringe_exc2_cols = crit.listb.syringe.exclusion2_strings()
    syringe_exc3_cols = crit.listb.syringe.exclusion3_strings()
    shotup_inc_cols = crit.listb.shotup.inclusion_strings()
    presc_inc_cols = crit.listb.presc.inclusion_strings()
    snort_inc_cols = crit.listb.snort.inclusion_strings()
    inject_inc_cols = crit.listb.inject.inclusion_strings()
    inject_exc1_cols = crit.listb.inject.exclusion1_strings()
    inject_exc2_cols = crit.listb.inject.exclusion2_strings()
    inject_exc3_cols = crit.listb.inject.exclusion3_strings()
    crush_inc_cols = crit.listb.crush.inclusion_strings()
    crush_exc1_cols = crit.listb.crush.exclusion1_strings()
    misuse_inc_cols = crit.listb.misuse.inclusion_strings()
    misuse_exc1_cols = crit.listb.misuse.exclusion1_strings()
    rec_inc_cols = crit.listb.recreational.inclusion_strings()
    rec_exc1_cols = crit.listb.recreational.exclusion1_strings()
    # we can ignore lint errors since we only pull the inc/exc we know exist
    df = df.with_columns(
        [
            (pl.any(pl.col(lista_cols) > 0)).alias("lvl2_crit5_lista"),
            (pl.any(pl.col(listc_cols) > 0)).alias("lvl2_crit5_listc"),
            (
                pl.any(pl.col(needle_inc_cols) > 0)
                & pl.any(pl.col(needle_exc1_cols) > 0).is_not()  # type: ignore
                | (
                    pl.any(pl.col(needle_exc2_cols) > 0).is_not()  # type: ignore
                    & pl.any(pl.col(needle_exc3_cols) > 0).is_not()  # type: ignore
                )
            ).alias("lvl2_crit5_needle"),
            (
                pl.any(pl.col(syringe_inc_cols) > 0)
                & pl.any(pl.col(syringe_exc1_cols) > 0).is_not()  # type: ignore
                | (
                    pl.any(pl.col(syringe_exc2_cols) > 0).is_not()  # type: ignore
                    & pl.any(pl.col(syringe_exc3_cols) > 0).is_not()  # type: ignore
                )
            ).alias("lvl2_crit5_syringe"),
            pl.any(pl.col(shotup_inc_cols) > 0).alias("lvl2_crit5_shotup"),
            pl.any(pl.col(presc_inc_cols) > 0).alias("lvl2_crit5_presc"),
            pl.any(pl.col(snort_inc_cols) > 0).alias("lvl2_crit5_snort"),
            (
                pl.any(pl.col(inject_inc_cols) > 0)
                & pl.any(pl.col(inject_exc1_cols) > 0).is_not()  # type: ignore
                | (
                    pl.any(pl.col(inject_exc2_cols) > 0).is_not()  # type: ignore
                    & pl.any(pl.col(inject_exc3_cols) > 0).is_not()  # type: ignore
                )
            ).alias("lvl2_crit5_inject"),
            (
                pl.any(pl.col(crush_inc_cols) > 0)
                & pl.any(pl.col(crush_exc1_cols) > 0).is_not()  # type: ignore
            ).alias("lvl2_crit5_crush"),
            (
                pl.any(pl.col(misuse_inc_cols) > 0)
                & pl.any(pl.col(misuse_exc1_cols) > 0).is_not()  # type: ignore
            ).alias("lvl2_crit5_misuse"),
            (
                pl.any(pl.col(rec_inc_cols) > 0)
                & pl.any(pl.col(rec_exc1_cols) > 0).is_not()  # type: ignore
            ).alias("lvl2_crit5_recreational"),
        ]
    )
    df = df.with_columns(
        [
            pl.any(
                pl.col(
                    [
                        "lvl2_crit5_needle",
                        "lvl2_crit5_syringe",
                        "lvl2_crit5_shotup",
                        "lvl2_crit5_presc",
                        "lvl2_crit5_snort",
                        "lvl2_crit5_inject",
                        "lvl2_crit5_crush",
                        "lvl2_crit5_misuse",
                        "lvl2_crit5_recreational",
                    ]
                )
            ).alias("lvl2_crit5_listb")
        ]
    )
    df = df.with_columns(
        [
            pl.when(
                # minimum criteria is lista and one of listb
                pl.col("lvl2_crit5_lista")
                & pl.col("lvl2_crit5_listb"),
            )
            .then(
                pl.when(
                    # if listb match was snort, don't check listc
                    pl.col("lvl2_crit5_snort"),
                )
                .then(True)
                .when(
                    # if listb match was NOT snort, check listc, true if no listc
                    pl.col("lvl2_crit5_listc").is_not(),
                )
                .then(True)
                .otherwise(False)
            )
            .otherwise(False)
            .alias("lvl2_crit5")
        ]
    )
    return df


def _aggregate_level2(df: pl.LazyFrame, c: BettanoOptions) -> pl.LazyFrame:
    criteria_list = c._patterns.level2.criteria_list()
    for i, criteria in enumerate(criteria_list, start=1):
        if isinstance(criteria, SimpleCriteria):
            df = df.pipe(_tag_simple_criteria, crit=i, lvl=2, criteria=criteria)
        elif isinstance(criteria, DoubleNegativeCriteria):
            inc_cols = criteria.inclusion_strings()
            exc1_cols = criteria.exclusion1_strings()
            exc2_cols = criteria.exclusion2_strings()
            df = df.with_columns(
                [
                    (
                        pl.any(pl.col(inc_cols) > 0)
                        & pl.any(pl.col(exc1_cols) > 0).is_not()
                        & pl.any(pl.col(exc2_cols) > 0).is_not()
                    ).alias(f"lvl2_crit{i}")
                ]
            )
        elif isinstance(criteria, DoublePositiveCriteria):
            inc1_cols = criteria.inclusion1_strings()
            inc2_cols = criteria.inclusion2_strings()
            exc_cols = criteria.exclusion_strings()
            df = df.with_columns(
                [
                    (
                        pl.any(pl.col(inc1_cols) > 0)
                        & pl.any(pl.col(inc2_cols) > 0)
                        & pl.any(pl.col(exc_cols) > 0).is_not()
                    ).alias(f"lvl2_crit{i}")
                ]
            )
        elif isinstance(criteria, Criteria5):
            df = df.pipe(_handle_criteria5, crit=criteria)

    df = df.with_columns(
        [(pl.any(pl.col(r"^lvl2_crit\d$")) & pl.col(">14")).alias("lvl2")]
    )
    return df


def _handle_triple_positive_age(
    df: pl.LazyFrame, crit: TriplePositiveCriteria, crit_num: int
) -> pl.LazyFrame:
    inc1_cols = crit.inclusion1_strings()
    inc2_cols = crit.inclusion2_strings()
    inc3_cols = crit.inclusion3_strings()
    # we can assume age cols exists
    df = df.with_columns(
        [
            pl.when(pl.any(pl.col(inc1_cols) > 0))
            .then(True)
            .when(pl.any(pl.col(inc2_cols) > 0) & pl.col("<46"))
            .then(True)
            .when(pl.any(pl.col(inc3_cols) > 0) & pl.col("<55"))
            .then(True)
            .otherwise(False)
            .alias(f"lvl2_crit{crit_num}")
        ]
    )
    return df


def _handle_two_part(
    df: pl.LazyFrame, crit: TwoPartCriteria, crit_num: int
) -> pl.LazyFrame:
    inc1_cols = crit.inclusion1_strings()
    inc2_cols = crit.inclusion2_strings()
    exc1_cols = crit.exclusion1_strings()
    exc2_cols = crit.exclusion2_strings()
    df = df.with_columns(
        [
            pl.when(
                (pl.any(pl.col(inc1_cols) > 0) & pl.any(pl.col(exc1_cols) > 0).is_not())
            )
            .then(True)
            .when(
                (pl.any(pl.col(inc2_cols) > 0) & pl.any(pl.col(exc2_cols) > 0).is_not())
            )
            .then(True)
            .otherwise(False)
        ]
    )
    return df


def _aggregate_level3(df: pl.LazyFrame, c: BettanoOptions) -> pl.LazyFrame:
    criteria_list = c._patterns.level3.criteria_list()
    for i, criteria in enumerate(criteria_list, start=1):
        if isinstance(criteria, SimpleCriteria):
            df = df.pipe(_tag_simple_criteria, crit=i, lvl=3, criteria=criteria)
        elif isinstance(criteria, TriplePositiveCriteria):
            df = df.pipe(_handle_triple_positive_age, crit=criteria, crit_num=i)
        elif isinstance(criteria, TwoPartCriteria):
            df = df.pipe(_handle_two_part, crit=criteria, crit_num=i)

    df = df.with_columns(
        [(pl.any(pl.col(r"^lvl3_crit\d$")) & pl.col(">14")).alias("lvl3")]
    )
    return df


def search_bettano(
    table: pl.LazyFrame | pl.DataFrame | pd.DataFrame,
    search_col: str,
    age_col: str | None = None,
    tracing: bool = False,
    levels: Literal[1, 2, 3] = 3,
) -> pl.LazyFrame:
    config = BettanoOptions(
        text_col=search_col, age_col=age_col, tracing=tracing, depth=levels
    )
    _check_col(df=table, col=config.text_col)
    lazy_df = to_lazy(table)
    original_cols = lazy_df.columns

    # ! done with setup / checks

    # search all potential terms needed for level requested
    lazy_df = lazy_df.with_columns(
        [
            # add A LOT of columns
            # but only those needed for the depth requested (unique as well)
            pl.col(config.text_col).str.count_match(rf"(?i){term}").alias(term)
            for term in config._patterns.get_combined_strings(depth=config.depth)
        ]
    )
    # create aggregates for each level requested
    match config.depth:
        case 1:
            lazy_df = lazy_df.pipe(_aggregate_level1, c=config)
        case 2:
            if config.age_col is None:
                raise ValueError("age_col must be provided for depth >1")
            _check_col(df=table, col=config.age_col)
            lazy_df = _add_age_columns(
                df=lazy_df, age_col=config.age_col
            ).with_row_count(name="row_count")
            lvl1 = lazy_df.pipe(_aggregate_level1, c=config)
            lvl2 = lvl1.filter(pl.col("lvl1")).pipe(_aggregate_level2, c=config)
            lazy_df = lazy_df.join(
                lvl1.select(pl.col("^lvl1.*$|^row_count$")),
                how="left",
                on="row_count",
            )
            lazy_df = lazy_df.join(
                lvl2.select(pl.col("^lvl2.*$|^row_count$")),
                how="left",
                on="row_count",
            )
            # no longer need row_count
            lazy_df = lazy_df.drop(columns=["row_count"])
        case 3:
            if config.age_col is None:
                raise ValueError("age_col must be provided for depth >1")
            _check_col(df=table, col=config.age_col)
            lazy_df = _add_age_columns(
                df=lazy_df, age_col=config.age_col
            ).with_row_count(name="row_count")
            lvl1 = lazy_df.pipe(_aggregate_level1, c=config)
            lvl2 = lvl1.filter(pl.col("lvl1")).pipe(_aggregate_level2, c=config)
            lvl3 = lvl2.filter(pl.col("lvl2")).pipe(_aggregate_level3, c=config)
            lazy_df = lazy_df.join(
                lvl1.select(pl.col("^lvl1.*$|^row_count$")),
                how="left",
                on="row_count",
            )
            lazy_df = lazy_df.join(
                lvl2.select(pl.col("^lvl2.*$|^row_count$")),
                how="left",
                on="row_count",
            )
            lazy_df = lazy_df.join(
                lvl3.select(pl.col("^lvl3.*$|^row_count$")),
                how="left",
                on="row_count",
            )
            lazy_df = lazy_df.drop(columns=["row_count"])
    match config.tracing:
        case False:
            additional_cols = ["lvl1", "lvl2", "lvl3"][: config.depth]
            lazy_df = lazy_df.select(original_cols + additional_cols)
        case True:
            warnings.warn(RuntimeWarning(_BETTANO_WARNING_TEXT), stacklevel=2)
    return lazy_df


def test_cdc_no_tracing():
    df = pd.read_csv("fake.csv")
    result = search_cdc(table=df, search_col="text", tracing=False)
    result = result.collect()
    df_cols = list(df.columns)
    result_cols = result.columns
    df_row_count, df_col_count = df.shape
    result_row_count, result_col_count = result.shape
    assert (
        df_row_count == result_row_count
    ), f"Not traced has uneven row count {result_row_count}, expected {df_row_count}"
    assert (
        result_col_count == df_col_count + 1
    ), f"Not traced has unexpected column count {result_col_count}, expected {df_col_count} + 1"
    # ignore:
    assert (
        df_cols + ["signal"] == result_cols
    ), f"Not traced columns contain more than expected. Expected {df_cols + ['signal']}, got {result_cols}"


def test_cdc_with_tracing():
    df = pd.read_csv("fake.csv")
    result = search_cdc(table=df, search_col="text", tracing=True)
    result = result.collect()
    df_cols = list(df.columns)
    result_cols = result.columns
    df_row_count, df_col_count = df.shape
    result_row_count, result_col_count = result.shape
    assert (
        df_row_count == result_row_count
    ), f"Not traced has uneven row count {result_row_count}, expected {df_row_count}"
    assert (
        result_col_count > df_col_count + 1
    ), f"Not traced has unexpected column count {result_col_count}, expected > ({df_col_count} + 1)"
    assert all(
        c in result_cols for c in df_cols
    ), "Not all original columns in traced results"
    assert len(result_cols) > len(df_cols), "Traced results have no extra columns"


def test_cdc_results():
    df = pd.read_csv("fake.csv")
    not_traced_matches = (
        search_cdc(table=df, search_col="text", tracing=False)
        .filter(pl.col("signal"))
        .collect()
        .shape[0]
    )
    traced_matches = (
        search_cdc(table=df, search_col="text", tracing=True)
        .filter(pl.col("signal"))
        .collect()
        .shape[0]
    )
    assert traced_matches > 0, "Traced results have no matches"
    assert not_traced_matches > 0, "Not traced results have no matches"
    assert (
        not_traced_matches == traced_matches
    ), f"Traced and not traced results do not match, traced: {traced_matches}, not traced: {not_traced_matches}"


def test_bettano_lvl1_no_tracing():
    df = pd.read_csv("fake.csv")
    result = search_bettano(table=df, search_col="text", tracing=False, levels=1)
    result = result.collect()
    df_cols = list(df.columns)
    result_cols = result.columns
    df_row_count, df_col_count = df.shape
    result_row_count, result_col_count = result.shape
    assert (
        df_row_count == result_row_count
    ), f"Not traced has uneven row count {result_row_count}, expected {df_row_count}"
    assert (
        result_col_count == df_col_count + 1
    ), f"Not traced has unexpected column count {result_col_count}, expected {df_col_count} + 1"
    assert (
        df_cols + ["lvl1"] == result_cols
    ), f"Not traced columns contain unexpected, got {result_cols}, expected {df_cols + ['lvl1']}"


def test_bettano_lvl2_no_tracing():
    df = pd.read_csv("fake.csv")
    result = search_bettano(
        table=df, search_col="text", age_col="age", tracing=False, levels=2
    )
    result = result.collect()
    df_cols = list(df.columns)
    result_cols = result.columns
    df_row_count, df_col_count = df.shape
    result_row_count, result_col_count = result.shape
    assert (
        df_row_count == result_row_count
    ), f"Not traced has uneven row count {result_row_count}, expected {df_row_count}"
    assert (
        result_col_count == df_col_count + 2
    ), f"Not traced has unexpected column count {result_col_count}, expected {df_col_count} + 2"
    assert (
        df_cols + ["lvl1", "lvl2"] == result_cols
    ), f"Not traced columns contain unexpected, got {result_cols}, expected {df_cols + ['lvl1', 'lvl2']}"


def test_bettano_lvl3_no_tracing():
    df = pd.read_csv("fake.csv")
    result = search_bettano(
        table=df, search_col="text", age_col="age", tracing=False, levels=3
    )
    result = result.collect()
    df_cols = list(df.columns)
    result_cols = result.columns
    df_row_count, df_col_count = df.shape
    result_row_count, result_col_count = result.shape
    assert (
        df_row_count == result_row_count
    ), f"Not traced has uneven row count {result_row_count}, expected {df_row_count}"
    assert (
        result_col_count == df_col_count + 3
    ), f"Not traced has unexpected column count {result_col_count}, expected {df_col_count} + 3"
    assert (
        df_cols + ["lvl1", "lvl2", "lvl3"] == result_cols
    ), f"Not traced columns contain unexpected, got {result_cols}, expected {df_cols + ['lvl1', 'lvl2', 'lvl3']}"


def test_bettano_levels_with_tracing():
    df = pd.read_csv("fake.csv")
    levels: list[Literal[1, 2, 3]] = [1, 2, 3]
    for level in levels:
        result = search_bettano(
            table=df, search_col="text", age_col="age", tracing=True, levels=level
        )
        result = result.collect()
        df_row_count, df_col_count = df.shape
        result_row_count, result_col_count = result.shape
        assert (
            df_row_count == result_row_count
        ), f"Not traced (level={level}) has uneven row count {result_row_count}, expected {df_row_count}"
        assert (
            result_col_count > df_col_count + level
        ), f"Not traced (level={level}) has unexpected column count {result_col_count}, expected > {df_col_count + level}"


def test_bettano_results():
    df = pd.read_csv("fake.csv")
    levels: list[Literal[1, 2, 3]] = [1, 2, 3]
    for level in levels:
        not_traced_matches = (
            search_bettano(
                table=df, search_col="text", age_col="age", tracing=False, levels=level
            )
            .filter(pl.col(f"lvl{level}"))
            .collect()
            .shape[0]
        )
        traced_matches = (
            search_bettano(
                table=df, search_col="text", age_col="age", tracing=True, levels=level
            )
            .filter(pl.col(f"lvl{level}"))
            .collect()
            .shape[0]
        )
        assert traced_matches > 0, "Traced results have no matches (level={level}))"
        assert (
            not_traced_matches > 0
        ), "Not traced results have no matches (level={level}))"
        assert (
            not_traced_matches == traced_matches
        ), f"Traced and not traced results do not match (level={level}), traced: {traced_matches}, not traced: {not_traced_matches}"


if __name__ == "__main__":
    pl.Config().set_fmt_str_lengths(500)
    df = pl.scan_csv("fake.csv")
    # run both

    time_diffs = []
    for opt in [True, False]:
        start = time.time()
        result = search_cdc(table=df, search_col="text", tracing=opt).collect()
        end = time.time()
        diff = end - start
        time_diffs.append(diff)
        matches = result.filter(pl.col("signal"))
        match_count = matches.shape[0]
        print("SAMPLE:")
        print(matches.sample())
        print(
            f"Found {match_count:,} results in [blue]{diff:.2f}[/blue]s (tracing={opt}))"
        )
    tracing_time, not_traced_time = time_diffs
    time_multiplier = tracing_time / not_traced_time
    print(f"Tracing took {time_multiplier:.2f} times longer than not tracing")
    # below just shows codes from json file :)
    # commented out because not really needed and hard to read test results
    # print(f"CDC Patterns:")
    # CDCPatterns.parse_file("cdc_patterns.json").show_patterns()

    # repeat for bettano
    levels: list[Literal[1, 2, 3]] = [1, 2, 3]
    for level in levels:
        time_diffs = []
        for opt in [True, False]:
            start = time.time()
            result = search_bettano(
                table=df,
                search_col="text",
                age_col="age",
                tracing=opt,
                levels=level,
            ).collect()
            end = time.time()
            diff = end - start
            time_diffs.append(diff)
            matches = result.filter(pl.col(f"lvl{level}"))
            match_count = matches.shape[0]
            print(f"LEVEL {level} SAMPLE:")
            print(matches.sample())
            print(
                f"Found {match_count:,} results in [blue]{diff:.2f}[/blue]s (tracing={opt} - level={level}))"
            )
        print(f"LEVEL {level} took {sum(time_diffs) / 2:.2f}s")
        tracing_time, not_traced_time = time_diffs
        time_multiplier = tracing_time / not_traced_time
        print(
            f"Level {level} tracing took {time_multiplier:.2f} times longer than not tracing"
        )
    # below just shows codes from json file :)
    # commented out because not really needed and hard to read test results
    # print(f"Bettano Patterns:")
    # BettanoPatterns.parse_file("bettano_patterns.json").show_patterns()
