
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
from pyspark.sql import SQLContext 
from pyspark.sql.functions import desc
from pyspark.streaming import StreamingContext


# In[ ]:


sc = SparkContext() # creating spark context


# In[ ]:


ssc = StreamingContext(sc , 10)
sqlcontext = SQLContext(sc) #creating sql context 


# In[ ]:


socket_stream = ssc.socketTextStream("127.0.0.1",5555) 


# In[ ]:


lines = socket_stream.window(20)


# In[ ]:


from collections import namedtuple
fields = ("tag", "count" ) 
Tweet = namedtuple( 'Tweet', fields )


# In[ ]:


text = lines.flatMap(lambda text: text.split(" ")) #splits the lines into the word


# In[ ]:


x = text.filter(lambda word: word.lower().startswith("#")) #separates the words starting with '#'


# In[ ]:


y = x.map(lambda word :(word.lower(),1))#maps the word with number 1 ex.(trending,1)


# In[ ]:


z = y.reduceByKey(lambda a,b :a+b)


# In[ ]:


a = z.map(lambda record :Tweet(record[0],record[1])) # count and hashtag gets stored in the tweet object 


# In[ ]:


b = a.foreachRDD(lambda rdd:rdd.toDF().sort(desc("count")).limit(10).registerTempTable("Tweet")) #Top 10 hashtags are stored in temporary table 'Tweet' along with their count


# In[ ]:


ssc.start() #starts the spark streaming session


# In[ ]:


import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline  #importing required modules')


# In[ ]:


count = 0
while count < 5:
    
    time.sleep( 3 )
    top_10_tweets = sqlcontext.sql( 'Select tag, count from Tweet' ) #running query on the temporary table 'Tweet'
    top_10_df = top_10_tweets.toPandas() #converting the received records into dataframe 
    display.clear_output(wait=True) 
    plt.figure(figsize=(20,20))

    sns.barplot(x="tag", y="count", data=top_10_df)
    plt.show()
    count = count + 1


# In[ ]:


ssc.stop() #stops the spark streaming session 

