import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb',
                        password='adb2020');


def film_in_category(category_id: int) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|

    Tabela wynikowa ma być posortowana po tylule filmu i języku.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category_id, int):
        df1 = '''select 
                    film.title title,
                    language.name languge,
                    category.name category
                from 
                    film
                inner join 
                    language using(language_id)
                inner join
                    film_category using(film_id)
                inner join 
                    category using(category_id)
                where
                    category.category_id = {id}
                order by 
                    film.title, language.name asc;'''.format(id = category_id)
        df = pd.read_sql(df1, con=connection)
        return df
    else:
        return None


def number_films_in_category(category_id: int) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  |

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category_id, int):
        df1 = f'''select 
                    category.name category, 
                    count(category.name)
                from 
                    film  
                inner join 
                    film_category using(film_id)
                inner join 
                    category using(category_id)   
                where 
                    category.category_id = {category_id}  
                group by 
                    category.name
                    '''.format(category_id= category_id)
        df = pd.read_sql(df1, con=connection)
        return df
    return None


def number_film_by_length(min_length: Union[int, float] = 0, max_length: Union[int, float] = 1e6):
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  |

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(min_length, (int, float)) and isinstance(max_length, (int, float)):
        if max_length > min_length:
            df1 = f'''
            select 
                film.length length, 
                count(film.length) from film
            where 
                film.length between '{min_length}' and '{max_length}'
            group by 
                film.length 
                '''.format()
            df = pd.read_sql(df1, con=connection)
            return df
        else:
            return None
    else:
        return None


def client_from_city(city: str, city_=None) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams

    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(city, str):
        df1 = f'''select 
                    city.city, 
                    customer.first_name, 
                    customer.last_name
                 from 
                    city 
                 inner join 
                    address using(city_id)
                 inner join 
                    customer using(address_id) 
                 where 
                    city.city = '{city}'
                 order by 
                    customer.first_name asc, customer.last_name asc
            '''.format()
        df = pd.read_sql(df1, con=connection)
        return df
    else:
        return None


def avg_amount_by_length(length: Union[int, float]) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389


    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(length, (int, float)):
        df1 = f'''select 
                    film.length length, 
                    avg(payment.amount)
                from 
                    film
                inner join 
                    inventory using(film_id)
                inner join 
                    rental using(inventory_id)
                inner join 
                    payment using(rental_id) 
                where 
                    length = '{length}'
                group by
                    film.length
                    '''.format()
        df = pd.read_sql(df1, con=connection)
        return df
    else:
        return None


def client_by_sum_length(sum_min: Union[int, float]) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265

    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(sum_min, (int,float)):
        df1 = '''SELECT 
                    customer.first_name first_name, 
                    customer.last_name last_name, 
                    sum(film.length) from film
                inner join 
                    inventory using(film_id) 
                inner join 
                    rental using(inventory_id)  
                inner join 
                    customer using(customer_id) 
                group by
                    customer.first_name, customer.last_name
                having
                    sum(film.length) > '{sum_min_}'
                order by 
                    sum(film.length), customer.last_name, customer.first_name
                    '''.format(sum_min_=sum_min)
        df = pd.read_sql(df1, con=connection)
        return df
    else:
        return None


def category_statistic_length(name: str) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(name, str):
        df1 = f'''select 
                    category.name category, 
                    avg(film.length), 
                    sum(film.length), 
                    min(film.length), 
                    max(film.length)
                from 
                    category  
                inner join 
                    film_category using(category_id) 
                inner join 
                    film using(film_id) 
                where 
                    category.name = '{name}'  
                group by 
                    category.name 
                    '''.format(name_=name)
        df = pd.read_sql(df1, con=connection)
        return df
    else:
        return None