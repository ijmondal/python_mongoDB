from wsgiref.validate import validator
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint
import certifi
from datetime import datetime as dt

load_dotenv(find_dotenv())

# password = os.environ.get("MONGODB_PWD")
# connection_string = f"mongodb+srv://indro:{password}@tutorial.ffav0cu.mongodb.net/?retryWrites=true&w=majority&authSource=admin"

printer = pprint.PrettyPrinter()
client = MongoClient("localhost",27017)
dbs = client.list_database_names()
# print(dbs)
production = client.production

def create_book_collection():
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publish_date", "type"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be a objectId and is required",
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required",
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "can only be one of the enm values and is required"
                },
            }
        }
    }

    try:
        production.create_collection("book")
    except Exception as e:
        print(e)

    production.command("collMod", "book", validator=book_validator)

# create_book_collection()

def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
            }
        }
    }

    try:
        production.create_collection("author")
    except Exception as e:
        print(e)
    
    production.command("collMod", "author", validator=author_validator)


# create_author_collection()

def create_data():
    authors = [
        {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": dt(1980,7,20)
        },
        {
            "first_name": "Jane",
            "last_name": "Smith",
            "date_of_birth": dt(1903,6,25)
        },
        {
            "first_name": "Mary",
            "last_name": "Jane",
            "date_of_birth": dt(1983,9,24)
        }
    ]

    author_collection = production.author
    # author_ids = author_collection.insert_many(authors).inserted_ids
    author_ids = author_collection.find().distinct("_id")

    print(author_ids)
    books = [
        {
            "title": "Book 1",
            "authors": [author_ids[0]],
            "publish_date": dt.today(),
            "type": "Fiction"
        },
        {
            "title": "Book 2",
            "authors": [author_ids[1]],
            "publish_date": dt(2019,5,17),
            "type": "Non-Fiction"
        },
        {
            "title": "Book 3",
            "authors": [author_ids[2]],
            "publish_date": dt(2020,5,21),
            "type": "Non-Fiction"
        }
    ]

    book_collection = production.book
    book_collection.insert_many(books).inserted_ids

# create_data()
# regex
def find_books_containing_k():
    books_containing_k = production.book.find({"title": {"$regex":"k{1}"}})
    printer.pprint(list(books_containing_k))

def find_authors_and_books():
    authors_and_books =  production.author.aggregate([{
        "$lookup": {
            "from": "book",
            "localField": "_id",
            "foreignField": "authors",
            "as": "books"
        }
    }])

    printer.pprint(list(authors_and_books))

def get_authors_books_count():
    authors_books_count =  production.author.aggregate([
        {
            "$lookup": {
                "from": "book",
                "localField": "_id",
                "foreignField": "authors",
                "as": "books"
            }
        },
        {
            "$addFields": {
                "total_books": {"$size": "$books"}
                }
        },
        {
            "$project": {"first_name": 1, "last_name": 1, "total_books": 1, "_id":0}
        }

    ])

    printer.pprint(list(authors_books_count))

def books_with_old_authors(gte_age=0, lte_age=50):
    books_containing_old_authors =  production.book.aggregate([
        {
            "$lookup": {
                "from": "author",
                "localField": "authors",
                "foreignField": "_id",
                "as": "authors"
            }
        },
        {
            "$set" : {
                "authors":{
                    "$map": {
                        "input": "$authors",
                        "in": {
                            "age": {
                                "$dateDiff": {
                                    "startDate": "$$this.date_of_birth",
                                    "endDate": "$$NOW",
                                    "unit": "year"
                                }
                            },
                            "first_name": "$$this.first_name",
                            "last_name": "$$this.last_name"
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "$and": [
                    {"authors.age": {"$gte":gte_age}},
                    {"authors.age": {"$lte":lte_age}}
                ]
            }
        },
        {
            "$sort": {
                "age": 1
            }
        }
    ])
    printer.pprint(list(books_containing_old_authors))

books_with_old_authors()