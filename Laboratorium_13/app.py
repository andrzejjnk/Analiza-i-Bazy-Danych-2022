from typing import List
from textblob import TextBlob


def hello(name: str) -> str:
    return f"Hello {name}"


def extract_sentiment(text):
    text = TextBlob(text)
    return text.sentiment.polarity


def text_contain_word(word: str, text: str) -> bool:
    return word in text


# Wykonanie ćwiczenia

def bubblesort(list_to_sort: List[int]) -> List[int]:
    if list_to_sort is None:
        return None
    else:
        if isinstance(list_to_sort, List):
            list_copy = list_to_sort[:]
            n = len(list_copy)
            swapped = False
            while n > 1:
                for i in range(1, n):
                    if list_copy[i - 1] > list_copy[i]:
                        list_copy[i - 1], list_copy[i] = list_copy[i], list_copy[i - 1]
                n -= 1
                swapped = True

            if not swapped:
                return None
            sorted_list = list_copy
            return sorted_list
        else:
            return None


