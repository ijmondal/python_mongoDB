from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint
import certifi

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://indro:{password}@tutorial.ffav0cu.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection_string, tlsCAFile=certifi.where())

dbs = client.list_database_names()
print(dbs)

test_db = client.test
collections = test_db.list_collection_names()
print(collections)


def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name": "Indro",
        "type": "Test"
    }

    inserted_id = collection.insert_one(test_document).inserted_id  # BSON object ID
    print(inserted_id)


# insert_test_doc()
#
production = client.production
person_collection = production.person_collection


def create_documents():
    first_names = ["Ram", "Shyam", "Mohan"]
    last_names = ["Kumar", "Reddy", "Singh"]
    ages = [21, 40, 23]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)

    person_collection.insert_many(docs)


# create_documents()

printer = pprint.PrettyPrinter()


def find_all_people():
    people = person_collection.find()
    print(people)  # pymongo cursor

    # person_list = list(people)
    # print(person_list)
    for person in people:
        printer.pprint(person)


# find_all_people()

def find_ram():
    ram = person_collection.find_one({
        "first_name": "Ram"
    })

    printer.pprint(ram)


# find_ram()

def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Number of people", count)


# count_all_people()

def get_person_by_id(person_id: str):
    from bson.objectid import ObjectId

    # for search using id, we have to convert str to bson object
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


# get_person_by_id("634bac26900d52006f6346df")

def get_age_range(min_age, max_age):
    query = {"$and": [
        {"age": {"$gte": min_age}},
        {"age": {"$lte": max_age}}
    ]}

    people = person_collection.find(query).sort("first_name")

    for person in people:
        printer.pprint(person)


# get_age_range(20, 35)

def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)

    for person in people:
        printer.pprint(person)


# project_columns()

def update_person_by_id(person_id: str):
    from bson.objectid import ObjectId

    # for search using id, we have to convert str to bson object
    _id = ObjectId(person_id)
    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"}
    }
    # person_collection.update_one({"_id":_id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})  # removes the field from the document


# update_person_by_id("634bac26900d52006f6346df")

def replace_one(person_id: str):
    from bson.objectid import ObjectId

    # for search using id, we have to convert str to bson object
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }

    person_collection.replace_one({"_id": _id}, new_doc)


# replace_one("634bac26900d52006f6346df")


def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    test_db.test.delete_one({"_id": _id})


# delete_doc_by_id("634ba2aab4ca96a58a9089ef")

# ---------------------------------------------------------------------------------------------

# RELATIONSHIPS

address = {
    "street": "random street",
    "number": 2345,
    "city": "Sricity"
}


def add_address_embed(person_id, _address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": _address}})


# add_address_embed("634bac26900d52006f6346df", address)

def add_address_relationship(person_id, _address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    temp_address = _address.copy()
    temp_address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(temp_address)

# add_address_relationship("634bac26900d52006f6346e0", address)
