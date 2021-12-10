



import nltk
from functools import lru_cache
import re
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string

class Preprocessor:
    def __init__(self):

        # self.stem = lru_cache(maxsize=10000)(nltk.LancasterStemmer('english').stem)
        self.stem = lru_cache(maxsize=10000)(nltk.SnowballStemmer('english').stem)
        self.stopwords = stopwords.words('english')

        #self.tokenize = nltk.tokenize.WhitespaceTokenizer().tokenize
        self.tokenize = nltk.tokenize.word_tokenize
        #self.lemma = lru_cache(maxsize=10000)(WordNetLemmatizer().lemmatize)
    def __call__(self, text):

        
        #normalization 
        
        text = text.lower()
        # text = re.sub(r'\d+', '', text)
        # tokenize
        str_list = text.split('<br /><br />')
        text = ''.join(str_list)
        tokens = self.tokenize(text)

        # tokens = [word for word in tokens if word.isalpha()]
        # stopwords
        tokens = [token for token in tokens if token not in self.stopwords]
        # punctuation

        tokens = [self.stem(token) for token in tokens]
        # lemma
        #tokens = [self.lemma(token) for token in tokens]
        return tokens

