"""*****************************************************************************

                         rgs_read_gold_standard_v2.py

Demonstration of how to read a Gold Standard file.

*****************************************************************************"""

from elasticsearch import Elasticsearch
import json

es = Elasticsearch( "http://localhost:9200" )
# Create the client. Make sure Elasticsearch is already running!

"""-----------------------------------------------------------------------------

Read a gold standard JSON file
"""

def rgs_show():
    with open( 'gold_standard_v2.json', 'r' ) as f:
        s = f.read()
        d = json.loads( s )
        print( d )

"""-----------------------------------------------------------------------------

Read a gold standard JSON file. json.loads() reads a string containing a JSON
and converts it to a Python dictionary. Returns this dictionary.

"""

def rgs_read():
    with open( 'gold_standard_v2.json', 'r' ) as f:
        s = f.read()
        d = json.loads( s )
    return d

"""-----------------------------------------------------------------------------

Shows how to process a dictionary containing a JSON and print out some information from it.

"""

def rgs_print():

    d = rgs_read()

    print( 'Number of queries: ', len( d[ "queries" ] ) )

    print( 'These are the queries:' )

    query_list = d[ "queries" ]
    for q in query_list:
        print( "Number:", q[ "number" ] )
        print( "Original query:", q[ "original_query" ] )
        print( "Keyword query:", q[ "keyword_query" ] )
        print( "Kibana query:", q[ "kibana_query" ] )
        print( "answer_type:", q[ "answer_type" ] )
        if not q[ "answer_type" ] in [ 'person', 'animal', 'organisation',\
            'event', 'place', 'date', 'reason', 'other' ]:
            print( 'ERROR: answer_type not valid',
                   'Must be exactly one of person, animal, organisation, event',
                   'place, date, reason, other.' )

        print( "Matching docs:", q[ "matches" ] )

"""-----------------------------------------------------------------------------

Goes through the queries, submitting them to Elasticsearch.

"""

def rgs_submit():

    d = rgs_read()

    print( 'Number of queries: ', len( d[ "queries" ] ) )

    query_list = d[ "queries" ]
    for q in query_list:
        kibana_query = q[ "kibana_query" ]
        # This is a query in Kibana Query Language
        # So we can submit it to Elasticsearch
        print( kibana_query )

        # Blank queries, i.e. {} crash Elastic...
        if kibana_query[ "query" ] != {}:
            result = es.search(
                index = 'student_index_v2',
                query = kibana_query[ "query" ])

        print( "Result of Elastic search:" )
        print( result )

rgs_print()  # Print out the list of queries etc. in the JSON
# rgs_submit() # Submit the kibana_query values from the JSON to Elasticsearch
