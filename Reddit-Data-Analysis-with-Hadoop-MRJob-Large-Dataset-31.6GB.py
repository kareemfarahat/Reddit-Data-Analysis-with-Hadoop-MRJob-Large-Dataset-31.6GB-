from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords 
from nltk import bigrams
import re
from statistics import mode
#import flair
# Most_frequent_subreddit class
class Most_frequent_subreddit(MRJob):
    
    def steps(self):
        return [     MRStep(mapper=self.maper_count_subreddit , combiner=self.combiner_count ,reducer=self.reducer_count),
                     MRStep(reducer=self.reducer_maxout)
          ]
        # mapper that return (key = subreddit, value = 1)
    def maper_count_subreddit(self, _, line):
        line = str(line)
        comment = json.loads(line)
        yield comment["subreddit"] , 1


        # combiner that return ((key = subreddit, sum(value)))
    def combiner_count(self, key, values):
        yield key, sum(values)

         # reducer that return ((key = subreddit, sum(value)))
    def reducer_count(self, key, values):
        yield None , (sum(values), key)
        
        #reducer that return a sorted array of [highst value element , key]
    def reducer_maxout(self, _ , values):
        yield None, sorted(values, reverse=True)[:10]
    



class Most_discussed_topic_per_subreddit(MRJob):
 
    # Function that take the query and lemmatizer object, return array of parsed query
    def query_parser(self, query):
        Result= []
        lemmatizer= WordNetLemmatizer()
        #normalizeing the query
        #lower casing all the letters
        query= query.lower() 
        # Removing punctuation characters
        query= re.sub(r'[^a-zA-Z1-9]',' ', query) 
        # Tokenizing the query
        query= word_tokenize(query)
        # Removing stop words
        query= [w for w in query if not w in stopwords.words("english")]
        for token in query:
            # lemmatizing the tokens
            Result.append(lemmatizer.lemmatize(token))
        bigrm = [' '.join(e) for e in bigrams(Result)]
        return bigrm
    # function that return the most frequent item on a list
    def most_common(self, List):
        return(mode(List))


    def steps(self):
        return [     MRStep(mapper=self.maper_Most_discussed_topics , combiner=self.combiner_Most_discussed_topics ,reducer=self.reducer_Most_discussed_topics),
                     MRStep(reducer=self.reducer_maxout),
                     MRStep(reducer=self.reducer_sort),

          ]
    def maper_Most_discussed_topics(self, _, line):
            line = str(line)
            comment = json.loads(line) #load the line in json format
            result = self.query_parser(comment["body"]) # parsing the line
            yield comment["subreddit"] , (result , 1) #return the subreddit name and the parsed comment


    def combiner_Most_discussed_topics(self, key, values):
        result = []
        s = 0
        # iterate over the values to extraxt the most frequent word
        for value in values:
            if len(value[0]): # check that the value is not empty
                result.append(self.most_common(value[0])) #get the most frequent bigram
                s += value[1] # the count on total comment in each subreddit
        yield key, (result, s)


    def reducer_Most_discussed_topics(self, key, values):
        result = []
        s= 0 
        for value in values:
            if len(value[0]):
                result.append(self.most_common(value[0]))
                s += value[1] 
        yield key, (result, s)

    def reducer_maxout(self, key , values):
        for value in values:
            if len(value[0]):            
                yield key, ( value[1], self.most_common(value[0])  )

    def reducer_sort(self, key, values):
        yield key, sorted(values, reverse=True)[:10]

class Most_discussed_topic_per_author(MRJob):  
 
    # Function that take the query and lemmatizer object, return array of parsed query
    def query_parser(self, query):
        Result= []
        lemmatizer= WordNetLemmatizer()
        #normalizeing the query
        #lower casing all the letters
        query= query.lower() 
        # Removing punctuation characters
        query= re.sub(r'[^a-zA-Z1-9]',' ', query) 
        # Tokenizing the query
        query= word_tokenize(query)
        # Removing stop words
        query= [w for w in query if not w in stopwords.words("english")]
        for token in query:
            # lemmatizing the tokens
            Result.append(lemmatizer.lemmatize(token))
        # generating bigram
        bigrm = [' '.join(e) for e in bigrams(Result)]
        return bigrm

    def most_common(self, List):
        return(mode(List))


    def steps(self):
        return [     MRStep(mapper=self.maper_Most_discussed_topics , combiner=self.combiner_Most_discussed_topics ,reducer=self.reducer_Most_discussed_topics),
                     MRStep(reducer=self.reducer_maxout),

          ]

    def maper_Most_discussed_topics(self, _, line):
            line = str(line)
            comment = json.loads(line) #load the line in json format
            result = self.query_parser(comment["body"]) # parsing the line
            if comment["author"] != "deleted": # check that the user acount was not deleted
                yield comment["author"] , (result , 1) #return the auther name and the parsed comment


    def combiner_Most_discussed_topics(self, key, values):
        result = []
        s = 0
        for value in values:
            if len(value[0]): # chek that the value is not empty
                result.append(self.most_common(value[0])) # get t
                s += value[1] 
        yield key, (result, s)


    def reducer_Most_discussed_topics(self, key, values):
        result = []
        s= 0 
        for value in values:
            if len(value[0]):
                result.append(self.most_common(value[0]))
                s += value[1] 
        yield key, (result, s)

    def reducer_maxout(self, key , values):
        for value in values:
            if len(value[0]):            
                yield key, ( self.most_common(value[0]), value[1]  )



class Rate_of_replies(MRJob):

    def steps(self):
            return [     MRStep(mapper=self.maper_replies , combiner=self.combiner_replies ,reducer=self.reducer_replies),
                         MRStep(reducer=self.reducer_controversiality),

            ]

    def maper_replies(self, _, line):
            line = str(line)
            comment = json.loads(line)
            if comment["parent_id"] != comment["link_id"]: # check if is it a reply
                yield (comment["controversiality"],comment["parent_id"]) , 1 # return controversiality and parent_id as composite key and 1 as value
            else: #or comment
                yield (comment["controversiality"],comment["parent_id"]) , 0 # return controversiality and parent_id as composite key and 0 as value


    def combiner_replies(self, key, values):
        yield key, sum(values)

    def reducer_replies(self, key, values):
        yield (key[0]), (sum(values))

    def reducer_controversiality(self, key, values):    
        yield key, sum(values) 

class Topics_with_highest_upvotes(MRJob):
      # Function that take the query and lemmatizer object, return array of parsed query
    def query_parser(self, query):
        Result= []
        lemmatizer= WordNetLemmatizer()
        #normalizeing the query
        #lower casing all the letters
        query= query.lower() 
        # Removing punctuation characters
        query= re.sub(r'[^a-zA-Z1-9]',' ', query) 
        # Tokenizing the query
        query= word_tokenize(query)
        # Removing stop words
        query= [w for w in query if not w in stopwords.words("english")]
        for token in query:
            # lemmatizing the tokens
            Result.append(lemmatizer.lemmatize(token))
        bigrm = [' '.join(e) for e in bigrams(Result)]
        return bigrm

    def most_common(self, List):
        return(mode(List))


    def steps(self):
        return [     MRStep(mapper=self.maper_topics , combiner=self.combiner_topic ,reducer=self.reducer_topic),
                     MRStep(reducer=self.reducer_maxout),

          ]

    def maper_topics(self, _, line):
            line = str(line)
            comment = json.loads(line)
            topic = self.query_parser(comment["body"])
            if len(topic) > 0:
                topic = self.most_common(topic)
                yield topic , comment["ups"]


    def combiner_topic(self, key , values):
        yield key , sum(values)

    def reducer_topic(self, key , values):
        yield None ,(sum(values), key)

    def reducer_maxout(self, _ , values):
        yield None, sorted(values, reverse=True)[:20]

class syntment_classifer(MRJob):

    flair_sentiment = flair.models.TextClassifier.load('en-sentiment') #loading the model

    def steps(self):
        return [     MRStep(mapper=self.maper_syntment_classifer , combiner=self.combiner_syntment_classifer ,reducer=self.reducer_syntment_classifer),

          ]
    def maper_syntment_classifer(self, _, line):
            line = str(line)
            comment = json.loads(line)
            s = flair.data.Sentence(comment["body"])
            self.flair_sentiment.predict(s)# getting the comment tag
            yield s.labels[0] , 1 #return a key that is (positive or negative) and value of 1

    def combiner_syntment_classifer(self,key,values):
        yield key, sum(values) # sum values by key

    def reducer_syntment_classifer(self,key,values):
        yield key, sum(values) # sum values by key









print('**********<<<<<<<<<<<< Most_discussed_topic_per_subreddit >>>>>>>>>>>>>>>>>>*************')    
Most_discussed_topic_per_subreddit.run()
print('**********<<<<<<<<<<<<   Most_discussed_topic_per_author  >>>>>>>>>>>>>>>>>>*************')  
Most_discussed_topic_per_author.run()
print('**********<<<<<<<<<<<< Most_frequent_subreddit >>>>>>>>>>>>>>>>>>*************')  
Most_frequent_subreddit.run()
print('**********<<<<<<<<<<<< Controversiality >>>>>>>>>>>>>>>>>>*************')  
Rate_of_replies.run()
print('**********<<<<<<<<<<<< Topics_with_highest_upvotes >>>>>>>>>>>>>>>>>>*************')  
Topics_with_highest_upvotes.run()

print ('************<<<<<<<<<<< comments classes >>>>>>>>>>>>*************')
#syntment_classifer.run()
