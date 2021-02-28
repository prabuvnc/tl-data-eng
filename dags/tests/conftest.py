import pytest


@pytest.fixture()
def imdb_cols():
    return ['title', 'budget', 'release_date', 'revenue', 'vote_average', 'production_companies', 'ratio']


@pytest.fixture()
def wiki_cols():
    return ['title', 'url', 'abstract']


@pytest.fixture()
def high_profit_movie_cols():
    return ['title', 'budget', 'release_date', 'revenue', 'vote_average', 'production_companies', 'ratio', 'url', 'abstract', 'load_timestamp']


@pytest.fixture()
def high_profit_movies():
    """
    Based on https://www.pajiba.com/seriously_random_lists/percentagewise-the-20-most-profitable-movies-of-all-time.php
    :return:
    """
    return ['Paranormal Activity', 'Tarnation', 'Mad Max', 'Super Size Me', 'The Blair Witch Project',
            'Night of the Living Dead', 'Rocky', 'Halloween', 'American Graffiti', 'Once', 'Napoleon Dynamite',
            'Friday the 13th', 'Open Water', 'Gone with the Wind', 'The Birth of a Nation',
            'The Big Parade', 'Saw', 'Primer', 'The Evil Dead']
