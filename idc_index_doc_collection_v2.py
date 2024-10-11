"""*****************************************************************************

                         idc_index_doc_collection_v2.py

Basic program to index .json collection using Python client.

V2 is adjusted to suit the Assignment doc collection.

*****************************************************************************"""

from datetime import datetime
from elasticsearch import Elasticsearch
import csv
import json

es = Elasticsearch( "http://localhost:9200" )
# Create the client. Make sure Elasticsearch is already running!

es.indices.create( index="student_index_v3" )
# Creates an empty index
# Once created, lasts forever
# Re-creating accidentally gives an error
# Once created, comment this out

"""-----------------------------------------------------------------------------

Creates an Elasticsearch index of some documents in .json.

1. You need a .json which has pairs of lines, the first gives the DocID, the second gives the document. Just like accounts.json we used before.

2. Make sure Elasticsearch is running

3. When you load this program, a Python client will connect to Elasticsearch (see above).

4. When you load this program, an index called 'student_index' will be created (See above). Before you re-load this file, you need to comment it out (see above).

5. Change the filename below, inside open( ... ) to the filename of your .json.

6. Run idc_index() . It should create the index.

7. Then you can search it - see below.

"""

def idc_index():

    with open( 'collection_1500_docs_per_topic_v2.json', 'r' ) as f:

        docid_json_str = f.readline()   # Read string containing JSON for ID
        content_json_str = f.readline() # Read string containing JSON for doc

        counter = 0
        while docid_json_str:

            docid_json = json.loads( docid_json_str ) # Convert strings to dicts
            content_json = json.loads( content_json_str )

            docid = docid_json[ 'index' ] ['_id' ]    # Extract the DocID
            # print( 'DocID:', docid )
            counter += 1
            if counter % 1000 == 0:
                print( '1000 docs done' )

            es.index(                                 # Index the doc
                index = 'student_index_v3',
                id = docid,
                document = content_json )

            # print( docid_json )
            # print( content_json )

            docid_json_str = f.readline()             # Read the next pair
            content_json_str = f.readline()

"""-----------------------------------------------------------------------------

Search your index. You need to create it first (see above). You can also search
any index your created previously with Kebana.

1. Load this program

2. Decide on a query, e.g. 'Makis Keravnos'

3. Do something like this:

>>> r = idc_search( 'makis keravnos' )
>>> r[ 'hits' ][ 'hits' ][ 0 ] # First hit - see 'title', 'parsedParagraphs'
>>> r[ 'hits' ][ 'hits' ][ 1 ] # Second hit
>>> # etc. (only two hits for this in the 10-doc collection)

4. Hint: To see the structure, do the same search in Kibana.

"""

def idc_search( query_string ):           # e.g. 'Makis Keravnos'

    result = es.search(
        index = 'student_index_v3',
        query = {
            'multi_match' : {
                'query' : query_string,
                "fields": [],             # 'title', 'parsedParagraphs' or both
                "type":'phrase'           # 'phrase' or 'best_fields'
            }
        } )

    return( result )

"""
To use this:
1. Check the .json document collection is the one you want to index!
   (at present it is 'collection_1500_docs_per_topic_v2.json')

2. Check that the name of the index you want to use is correct.
   (at present it is student_index_v3)

3. Check that this index does NOT already exist (use CURL).

4. Then you can call idc_index() (see below).

5. It should take about an hour.
"""

#idc_index
# Uncomment this and reload, having made the above checks!
