from pydantic import ValidationError
import pytest

from cabinet import cleaning_drawer


def test_categorize_age():
    # test string
    assert cleaning_drawer.categorize_age("10") == "<18"  # type: ignore
    assert cleaning_drawer.categorize_age(1) == "<18"
    assert cleaning_drawer.categorize_age(1) == "<18"
    assert cleaning_drawer.categorize_age(10) == "<18"
    assert cleaning_drawer.categorize_age(18) == "18-25"
    assert cleaning_drawer.categorize_age(25) == "18-25"
    assert cleaning_drawer.categorize_age(26) == "26-35"
    assert cleaning_drawer.categorize_age(35) == "26-35"
    assert cleaning_drawer.categorize_age(36) == "36-45"
    assert cleaning_drawer.categorize_age(45) == "36-45"
    assert cleaning_drawer.categorize_age(46) == "46-55"
    assert cleaning_drawer.categorize_age(55) == "46-55"
    assert cleaning_drawer.categorize_age(56) == "56-65"
    assert cleaning_drawer.categorize_age(65) == "56-65"
    assert cleaning_drawer.categorize_age(66) == ">65"
    assert cleaning_drawer.categorize_age(100) == ">65"
    assert pytest.raises(ValidationError, cleaning_drawer.categorize_age, -1)
