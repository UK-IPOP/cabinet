# rough draft for now
def categorize_age(age: int) -> str:
    assert type(age) == int, f"Age must be an integer, got {age} of type {type(age)}"
    assert age >= 0, f"Age must be a positive integer, got {age}"
    if age < 18:
        return "<18"
    elif age < 30:
        return "18-30"
    elif age < 40:
        return "30-40"
    else:
        return "40+"
    