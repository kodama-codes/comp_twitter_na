from pymongo import MongoClient


class MongoDB:
    """
    Mapper Class for the PyMongo interface
    """

    def __init__(self, host: str, port: int, db_name: str):
        try:
            self.client = MongoClient(host, port)
            self.mongoDB = self.client[db_name]
        except Exception as e:
            print("Connect to MongoDB failed: {}".format(str(e)))

    def get_create_collection(self, collection_name: str):
        """
        Get an existing collection or create a new one if it not already exists
        """

        return self.mongoDB[collection_name]
