'''
This is the classifier, it is used to classify the sentiment of a review
We should rename it to entry_point.py in the future

'''
from transformers import pipeline
from utils import service_keywords, keywords, menu_items_lookup, drink_items_lookup

class ArtificialWalla:
    '''
    This class is used to classify the sentiment of a review

    ---
    Methods:
       classify_review(review)

    ---
    Notes:
        Default truncations is at 512 -  it will fail for input size is greater than that.
        Setting `truncation` to true should avoid that error

    '''
    MODEL_PATH = "distilbert-base-uncased-finetuned-sst-2-english"
    SENTIMENT_TASK = pipeline("sentiment-analysis", model=MODEL_PATH, truncation=True)

    def __init__(self):
        self.review = None
        self.sentiment = None
        self.confidence = None
        
        self.keywords_lookup = keywords
        self.menu_items_lookup = menu_items_lookup
        self.drink_items_lookup = drink_items_lookup


    def classify_review(self, review):
        '''
        Modify the class to allow the model to predict a neutral sentiment.
        If the confidence is less than 0.75 is considered neutral
        '''
        self.menu_items = []
        self.drink_items = []
        self.keywords_found = []

        # check if there is a menu item in the review
        for item in self.menu_items_lookup:
            if str(item).lower() in str(review).lower():
                self.menu_items.append(str(item) + " -")

        # check if there is a drink item in the review
        for item in self.drink_items_lookup:
            if str(item).lower() in str(review).lower():
                self.drink_items.append(str(item) + " -")

        # check if there is a keyword in the review
        for keyword in self.keywords_lookup:
            if str(keyword).lower() in str(review).lower():
                self.keywords_found.append(str(keyword) + " -")

        if review == '':
            self.review = review
            self.sentiment = 'neutral'
            self.confidence = 0.0
            return self.sentiment, self.confidence, [],\
            [], []
        
        else:
            sentiment = self.SENTIMENT_TASK(review)
            if sentiment[0]['score'] < 0.75:
                sentiment[0]['label'] = 'neutral'

            self.review = review
            self.sentiment = sentiment[0]['label']
            self.confidence = round(sentiment[0]['score'], 2)
            return self.sentiment, self.confidence, self.menu_items,\
            self.keywords_found, self.drink_items

if __name__ == "__main__":
    walla = ArtificialWalla()
    walla.classify_review('I love this restaurant')
    print(walla.sentiment, walla.confidence, walla.menu_items,\
          walla.keywords_found, walla.drink_items)
    