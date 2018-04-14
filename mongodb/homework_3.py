import pprint
import pymongo
from pymongo import MongoClient

# TODO modify
client = MongoClient()
# client = MongoClient(host='127.0.0.1', port=27017)
database = client.test

collection = database.movies


def update_rated(collection):
    # update all
    retsult = collection.update_many({
        'genres': 'Comedy',  # 'genres': ['Comedy']
        'rated': 'NOT RATED'
    }, {
        '$set': {
            'rated': 'Pending rating'
        }
    })
    print(retsult.modified_count)


def insert_my_database(collection):
    # find one and insert one into Comedy.movies
    result = collection.find_one({
        'genres': 'Comedy'
    })
    insert_result = collection.insert_one({
        'title': result.get('title', ''),
        'year': result.get('year', ''),
        'countries': result.get('countries', []),
        'genres': result.get('genres'),
        'directors': result.get('directors', []),
        'imdb': result.get('imdb', {})
    })
    print(insert_result.acknowledged)


def statistics_my_genres_movies(collection):
    result = collection.aggregate([{
        '$match': {
            'genres': 'Comedy',
        }
    },
        {'$group': {'_id': 'Comedy', 'count': {'$sum': 1}}
         }], useCursor=False)

    pprint.pprint(list(result))


def statistics_movies_country(collection):
    result = collection.aggregate([{
        '$unwind': '$countries'
    }, {
        '$match': {'genres': 'Comedy'}
    }, {
        '$group': {'_id': {'country': '$countries', 'rating': 'Pending rating'}, 'count': {'$sum': 1}}
    }], useCursor=False)

    pprint.pprint(list(result))


def lookup_examples(database):
    col3 = database.col3
    col4 = database.col4

    col3.insert_many([
        {'_id': 1, 'title': 'Cinderella', 'year': 1889, 'type': 'movie', "genres": "Short"},
        {'_id': 2, 'title': 'The Sea', 'year': 1991, 'type': 'movie', 'genres': 'Comedy'},
        {'_id': 3, 'title': 'The House of the Devil', 'year': 1992, 'type': 'movie', 'genres': 'Documentary'}
    ])

    col4.insert_many([
        {'_id': 1, 'title': 'Cinderella', 'time': 1889, 'type': 'movie', "name": "Short"},
        {'_id': 2, 'title': 'The Sea', 'time': 1991, 'type': 'movie', 'name': 'Comedy'},
        {'_id': 3, 'title': 'The House of the Devil', 'time': 1993, 'type': 'movie', 'name': 'Documentary'}
    ])
    
    result = col3.aggregate([{
        '$lookup': {
            'from': "col4",
            'localField': "year",
            'foreignField': "time",
            'as': 'extra'
        }}
    ])
    pprint.pprint(list(result))

    result = col3.aggregate([{
        '$lookup': {
            'from': 'col4',
            'localField': '_id',
            'foreignField': '_id',
            'as': 'docs'
        }
    }])
    pprint.pprint(list(result))


if __name__ == '__main__':
    # A. Update all movies with "NOT RATED" at the "rated" key to be "Pending rating". The operation must be in-place and atomic
    # update_rated(collection)

    # B. Find a movie with your genre in imdb and insert it into your database with the fields listed in the hw description.
    # insert_my_database(database, collection, cilent)

    # C. Use the aggregation framework to find the total number of movies in your genre.
    # statistics_my_genres_movies(collection)

    # D. Use the aggregation framework to find the number of movies made in the country you were born in with a rating of "Pending rating"
    # statistics_movies_country(collection)

    # E. Create an example using the $lookup pipeline operator. See hw description for more info.
    # lookup_examples(database)
    pass
