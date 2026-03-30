import pytest
from galerna.cli import parse_cases

def test_parse_cases_single():
    assert parse_cases("1") == [1]
    assert parse_cases(" 4 ") == [4]

def test_parse_cases_comma_separated():
    assert parse_cases("1,2,3") == [1, 2, 3]
    assert parse_cases("1, 2, 3") == [1, 2, 3]

def test_parse_cases_range():
    assert parse_cases("1-5") == [1, 2, 3, 4, 5]
    assert parse_cases(" 1-3 ") == [1, 2, 3]
    # Spacing around hyphen works because int() strips whitespace
    assert parse_cases("1 - 3") == [1, 2, 3]

def test_parse_cases_mixed():
    assert parse_cases("1, 3-5, 8") == [1, 3, 4, 5, 8]

def test_parse_cases_unsorted_and_duplicates():
    # It should return a sorted list with unique items
    assert parse_cases("5,1,1-3") == [1, 2, 3, 5]

def test_parse_cases_empty_parts():
    # Trailing commas or duplicate commas
    assert parse_cases("1,,2,") == [1, 2]
    assert parse_cases("") == []
