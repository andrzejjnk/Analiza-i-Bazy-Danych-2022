import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category, int):
        df1 = f'''select 
                            film.title title, 
                            language.name  languge,
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
                            category.category_id = {category}
                        order by
                            film.title, language.name asc;
                            '''.format()
        df = pd.read_sql(df1, con=connection)
        return df
    if isinstance(category, str):
        df1 = f'''select 
                            film.title title, 
                            language.name  languge,
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
                            category.name like '{category}'
                        order by
                            film.title, language.name asc;
                            '''.format()
        df = pd.read_sql(df1, con=connection)
        return df

    else:
        return None
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category, int):
        df1 = f'''select 
                            film.title title, 
                            language.name  languge,
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
                            category.category_id = {category}
                        order by
                            film.title, language.name asc;
                            '''.format()
        df = pd.read_sql(df1, con=connection)
        return df
    if isinstance(category, str):
        df1 = f'''select 
                            film.title title, 
                            language.name  languge,
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
                            category.name ilike '{category}'
                        order by
                            film.title, language.name asc;
                            '''.format()
        df = pd.read_sql(df1, con=connection)
        return df

    else:
        return None
    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (str): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(title, str):
        df1 = f'''select 
                            first_name,
                            last_name
                        from 
                            actor
                        inner join 
                            film_actor using(actor_id)
                        inner join 
                            film using(film_id)
                        where 
                            film.title like '{title}'
                        order by
                            last_name, first_name
                            '''.format()
        df = pd.read_sql(df1, con=connection)
        return df

    else:
        return None


def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    if isinstance(words, list):
        string = '|'.join(words)
        df1 = f"""select 
                        title
                    from 
                        film
                    where 
                        title ~* '(?:^| )({string})""" + """{1,}(?:$| )'
                    order by 
                        title
                    """
        df = pd.read_sql(df1, con=connection)
        return df
    else:
        return None