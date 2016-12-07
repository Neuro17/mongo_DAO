__author__ = 'biagio'

import pymongo as pm
import pprint as pp
import argparse

 
class Mongo:

    def __init__(self, db, collection):
        self.client = pm.MongoClient()
        self.db = getattr(self.client, db)
        self.collection = getattr(self.db, collection)
        self.result = None

    def switch_collection(self, collection):
        return getattr(self.db, collection) if collection else self.collection

    def post_processing(self, order_by=None):
        pass

    def save_document(self, doc, collection=None):
        # TODO - replace save with insert_one

        """

        :param doc: the document to be saved.
        :param collection: Optional. Collection where document has to be saved.
        :return:
        """
        c = self.switch_collection(collection)
        # print('Saving document')
        # print('-' * 80)
        c.save(doc)

    def update_document(self, doc):
        pass

    def get_element_by_mongo_id(self, id, collection=None):
        c = getattr(self.db, collection) if collection else self.collection
        return c.find_one({'_id': id})

    def get_element_by_id(self, id, collection=None):
        c = getattr(self.db, collection) if collection else self.collection
        return c.find_one({'id_doc': id})

    def get_all(self, collection=None, order_by=None):
        c = getattr(self.db, collection) if collection else self.collection
        if order_by:
            return c.find({}).sort([(order_by, pm.ASCENDING)])
        return c.find({})

    def get_by_key(self, key, value, collection=None, order_by=None):
        c = getattr(self.db, collection) if collection else self.collection
        query = {key: value}
        if order_by:
            return c.find(query).sort([(order_by, pm.ASCENDING)])
        return c.find(query)

    def get_empty_abstract(self, collection=None, order_by=None):
        c = getattr(self.db, collection) if collection else self.collection
        query = {'abstracts': {'$exists': True, '$size': 0}}
        if order_by:
            return c.find(query).sort([(order_by, pm.ASCENDING)])
        else:
            return c.find(query)

    def get_doc_with_no_key(self, key, collection=None, order_by=None):
        c = getattr(self.db, collection) if collection else self.collection
        query = {key: {'$exists': False}}
        if order_by:
            return c.find(query).sort([(order_by, pm.ASCENDING)])
        else:
            return c.find(query)

    def is_empty(self, collection=None):
        c = getattr(self.db, collection) if collection else self.collection
        return c.count() == 0

    def remove_document_by_id(self, id, collection=None):
        c = getattr(self.db, collection) if collection else self.collection
        return c.delete_one({'id_doc': id})

    def custom_query(self, query, collection=None):
        c = getattr(self.db, collection) if collection else self.collection
        return c.find(query)

    def safe_mode(self, collection=None):
        c = getattr(self.db, collection) if collection else self.collection
        if not self.is_empty(collection):
            raise Exception('Operation denied: Database is not empty!')
        else:
            return True


def main():
    parser = argparse.ArgumentParser(
        description='Script that performs action on db')

    parser.add_argument('-d',
                        dest='dataset',
                        help='Dataset name',
                        required=True,
                        choices=['re0', 're1', 're0_for_alchemy',
                                 're1_for_alchemy'])

    parser.add_argument('--db',
                        dest='db',
                        help='DB name',
                        required=True,
                        choices=['hc'])

    parser.add_argument('--action', '-a',
                        dest='action',
                        help='specify action to perform',
                        required=True,
                        choices=['create', 'duplicate', 'clean_text'])

    args = parser.parse_args()

    dataset = args.dataset
    db = args.db
    action = args.action

    # if action == 'create':
    #     init_db(dataset, db)
    # elif action == 'duplicate':
    #     duplicate_db(dataset, db)
    # elif action == 'clean_text':
    #     clean_text(dataset, db)


if __name__ == '__main__':
    main()
