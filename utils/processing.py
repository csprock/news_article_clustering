import pandas as pd
from functools import reduce
import re

def get_top_tags(n, articles_df, tags_df):
    
    tag_counts = tags_df.groupby(['tag']).count().sort_values('id', ascending=False).iloc[0:n] # get top n tags
    tags_df_filtered = tags_df.loc[tags_df.tag.isin(tag_counts.index), :]                      # filter tags_df by top tags
    articles_df_filtered = articles_df.loc[articles_df.id.isin(tags_df_filtered.id), :]        # filter articles to ones that have a top tag
    
    return articles_df_filtered, tags_df_filtered

# tag processing
def wordify_tag(tag):
    return re.sub(' ', '', tag).strip()

def combine_words(series, sep=' '):
    return reduce(lambda x, y: x + sep + y, series)