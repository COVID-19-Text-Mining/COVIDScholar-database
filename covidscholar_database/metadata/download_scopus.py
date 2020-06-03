import os
import sys

parent_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '../..'
    )
)
print('parent_folder', parent_folder)
if parent_folder not in sys.path:
    sys.path.append(parent_folder)


import json
from pprint import pprint
from datetime import datetime
import os

from covidscholar_database.metadata.common_utils import get_mongo_db
from covidscholar_database.metadata.api_scopus import query_scopus_by_doi
from covidscholar_database.metadata.api_scopus import change_default_scopus_config


def collect_scopus_data(mongo_db):
    aug_col = mongo_db['metadata_from_api']

    all_doi = aug_col.find(
        {
            '$or': [
                {
                    'doi': {'$exists': True},
                    'scopus_raw_result': {'$exists': False},
                    'scopus_tried': {'$exists': False},
                },
                {
                    'doi': {'$exists': True},
                    'scopus_raw_result': {'$exists': False},
                    'scopus_tried': False,
                }
            ]
        },
        {
            '_id': True,
            'doi': True,
        }
    )
    print('total_num:', all_doi.count())

    num_blocks = 50
    block_size = 100
    for i in range(num_blocks):
        print('block index: {} out of {}'.format(i, num_blocks))
        # get tasks as a block
        block = aug_col.aggregate(
            [
                {
                    '$match': {
                        '$or': [
                            {
                                'doi': {'$exists': True},
                                'scopus_raw_result': {'$exists': False},
                                'scopus_tried': {'$exists': False},
                            },
                            {
                                'doi': {'$exists': True},
                                'scopus_raw_result': {'$exists': False},
                                'scopus_tried': False,
                            }
                        ]
                    }
                },
                {
                    '$sample': {'size': block_size}
                },
                {
                    '$project': {'_id': True, 'doi': True,}
                },
            ],
            allowDiskUse=True
        )

        # download from scopus
        for doc in block:
            print(doc)
            set_params = {
                'last_updated': datetime.now(),
                'scopus_tried': True
            }

            try:
                query_result = query_scopus_by_doi(doc['doi'])
            except Exception as e:
                query_result = None
                set_params['scopus_tried'] = False
                print('Error!', type(e), e)
            if query_result is not None:
                set_params['scopus_raw_result'] = query_result
            aug_col.find_one_and_update(
                {'_id': doc['_id']},
                {
                    '$set': set_params
                }
            )


if __name__ == '__main__':

    """
    Readme to download data from scopus

    1. Apply a scopus API key 
        (a) https://dev.elsevier.com/api_key_settings.html
        (b) Click "My API key" to register an account and get the key 

    2. Add following environment variables
        SCOPUS_API: xx-your-scopus-api-key-xx
        COVID_HOST: mongodb05.nersc.gov
        COVID_USER: xx-your-username-xx
        COVID_PASS: xx-your-password-xx
        COVID_DB: COVID-19-text-mining
    
    3. Run this script once a week. It will try to download data from scopus for 50 blocks * 100 doi/block.
    
    """

    db = get_mongo_db(mongo_config={
        'host': os.getenv("COVID_HOST"),
        'username': os.getenv("COVID_USER"),
        'password': os.getenv("COVID_PASS"),
        'db_name': os.getenv("COVID_DB"),
    })
    print(db.collection_names())

    # scrape scopus
    # scopus need some complex api setup
    change_default_scopus_config(
        api_key=os.getenv('SCOPUS_API')
    )
    collect_scopus_data(db)

