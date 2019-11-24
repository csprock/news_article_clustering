import os
import json
import pandas as pd
import psycopg2

def get_article_df(path):
    
    with open(path, 'r') as f:
        loaded_data = json.load(f)['data']
        
    ids = [None]*len(loaded_data)
    headlines = [None]*len(loaded_data)
    
    for i, article in enumerate(loaded_data):
        ids[i] = article['id']
        headlines[i] = article['text']
        
    return pd.DataFrame.from_dict({'id':ids, 'text':headlines})
        

def get_tag_df(path):
    
    with open(path, 'r') as f:
        loaded_data = json.load(f)['data']
        
    ids = list()
    tags = list()
    
    for article in loaded_data:
        
        _tags = article['tags']
        _ids = [article['id']]*len(_tags)
        
        ids.extend(_ids)
        tags.extend(_tags)
        
    return pd.DataFrame.from_dict({'id':ids, 'tag': tags})


def get_data(path):
    
    article_df = get_article_df(path)
    tag_df = get_tag_df(path)
    
    return article_df, tag_df


#### gather data from postgres ####

PGUSER = os.environ['PGUSER']
PGHOST = os.environ['PGHOST']
PGPORT = os.environ['PGPORT']
PGDATABASE = os.environ['PGDATABASE']
PGPASSWORD = os.environ['PGPASSWORD']

ARTICLE_QUERY = '''
SELECT 
	id,
	summary 
FROM articles 
WHERE id IN (
	SELECT 
		article_id 
	FROM keywords WHERE tag = 'subject')
	AND date BETWEEN '{start_date}' AND '{end_date}'
'''

TAG_QUERY = '''
SELECT 
	article_id, 
	keyword 
FROM keywords 
WHERE tag = 'subject'
AND
article_id IN (
	SELECT
		id
	FROM articles
	WHERE date BETWEEN '{start_date}' AND '{end_date}'
)
'''


def get_articles(start_date, end_date):
    
    query = ARTICLE_QUERY.format(start_date=start_date, end_date=end_date)
    
    conn = psycopg2.connect(
        user=PGUSER,
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        password=PGPASSWORD
    )

    
    with conn.cursor() as curs:    
        curs.execute(query)
        results = curs.fetchall()
        
    return results


def get_tags(start_date, end_date):
    
    query = TAG_QUERY.format(start_date=start_date, end_date=end_date)
    
    conn = psycopg2.connect(
        user=PGUSER,
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        password=PGPASSWORD
    )

    
    with conn.cursor() as curs:    
        curs.execute(query)
        results = curs.fetchall()
        
    return results