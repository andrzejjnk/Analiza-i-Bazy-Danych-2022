import pytest
from Laboratorium_13.app import hello, extract_sentiment, text_contain_word
from Laboratorium_13.app import bubblesort


def test_hello():
    got = hello("Aleksandra")
    want = "Hello Aleksandra"

    assert got == want


def test_extract_sentiment():
    text = "I think today will be a great day"
    sentiment = extract_sentiment(text)
    assert sentiment > 0


testdata = ["I think today will be a great day", "I do not think this will turn out well"]


@pytest.mark.parametrize('sample', testdata)
def test_extract_sentiment(sample):
    sentiment = extract_sentiment(sample)

    assert sentiment >= 0


testdata = [
    ('There is a duck in this text', 'duck', True),
    ('There is nothing here', 'duck', False)
]


@pytest.mark.parametrize('sample, word, expected_output', testdata)
def test_text_contain_word(sample, word, expected_output):
    assert text_contain_word(word, sample) == expected_output


# Wykonanie ćwiczenia
# przykladowe testy do algorytmu  bubblesort
testdata_bubblesort = [([3, 4, 5, 6, 1, 2], [1, 2, 3, 4, 5, 6]),
                       ([1, 1, 1, 1, 1, 1, 1, 1, 1], [1, 1, 1, 1, 1, 1, 1, 1, 1]),
                       ([], None),
                       ([3, 4, 5, 6, 7], [3, 4, 5, 6, 7]),
                       ([-5, 3, -4, -2, 0, 0, 0, 5, 3], [-5, -4, -2, 0, 0, 0, 3, 3, 5]),
                       (5, None),
                       (None, None)]


@pytest.mark.parametrize('list_to_sort, expected_output', testdata_bubblesort)
def test_bubblesort(list_to_sort, expected_output):
    assert bubblesort(list_to_sort) == expected_output
