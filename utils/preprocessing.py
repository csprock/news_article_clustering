import re
import string
from nltk.stem import SnowballStemmer

snowball = SnowballStemmer('english', ignore_stopwords=False)

def remove_numbers(text):
    return re.sub(r"\d+", "", text)

def remove_punctuation(text):
    translate_table = dict((ord(char), None) for char in string.punctuation)   
    return text.translate(translate_table)

def remove_stopwords(tokens, stop_words):
    return [w for w in tokens if w not in stop_words]

def apply_stemmer(tokens):
    stemmed = [snowball.stem(t) for t in tokens]
    
    return ' '.join(stemmed)