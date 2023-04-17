"""This module contains code for typical data normalization tasks.

We generally enforce 'type-saftey' by using pydantic's validate_arguments decorator
and other pydantic types.
"""
from pydantic import validate_arguments
from pydantic.types import PositiveInt


@validate_arguments
def categorize_age(age: PositiveInt) -> str:
    """Categorize an age into a string.

    Args:
        age (PositiveInt): Age to categorize. Must be a positive integer.

    Returns:
        str: Categorized age.

    Examples:
        >>> categorize_age(10)
        '<18'
        >>> categorize_age(18)
        '18-25'
        >>> # a general example using pandas
        >>> df['age'].apply(categorize_age)
        pandas.Series(['<18', '18-25', '26-35', '36-45', '46-55', '56-65', '65+'])
    """
    if age < 18:
        return "<18"
    elif age <= 25:
        return "18-25"
    elif age <= 35:
        return "26-35"
    elif age <= 45:
        return "36-45"
    elif age <= 55:
        return "46-55"
    elif age <= 65:
        return "56-65"
    else:
        return "65+"
