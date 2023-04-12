from pydantic import validate_arguments
from pydantic.types import PositiveInt


@validate_arguments
def categorize_age(age: PositiveInt) -> str:
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
