# Yelp DLA project step 5 for bigrams
# Created by Heejo Clare Keum
import nltk
import nltk.corpus
from nltk.collocations import *
import mysql.connector
from mysql.connector import errorcode
import collections
import math

#connect to yelpData database;
conn = mysql.connector.connect(user = "jeffreyklee", password = "cookies123", \
host = "jeffreyklee.mysql.pythonanywhere-services.com", database = "jeffreyklee$yelpData")
c = conn.cursor()

#Step5
#take the normalized word counts for each word or phrase (in a document),
#add 3/8, square root it, and then multiply by 2


c.execute("SELECT bigram FROM NumReviewersAllUniqueBigrams;")
bigramsFromStep4 = c.fetchall()
# Dictionary is faster to check if the bigram survived from Step4
bigramsFromStep4Dict = {}
for word in bigramsFromStep4:
    bigram = word[0]
    bigramsFromStep4Dict[bigram] = False # placeholder



c.execute("SELECT * FROM UniqueBigramsPerReviewWithID")
uniqueBigramsPerReviewWithID = c.fetchall()

#Step5.2: Save normalized and transformed bigram count
c.execute("DROP TABLE IF EXISTS `TransformedUniqueBigramsPerReview`")
create_TransformedUniqueBigramsPerReview = "CREATE TABLE TransformedUniqueBigramsPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "bigram VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "transformedWordCount FLOAT, " \
                         "PRIMARY KEY (`recordID`))"
insert_TransformedUniqueBigramsPerReview = "INSERT INTO TransformedUniqueBigramsPerReview "\
    "(recordID, reviewID, reviewerID, bigram, count, transformedWordCount) VALUES "\
    "(%s, %s, %s, %s, %s, %s)"
c.execute(create_TransformedUniqueBigramsPerReview)

transRecordID = 1
for row in uniqueBigramsPerReviewWithID:
    #print (row)
    recordID, reviewID, reviewerID, bigram, count = row
    #print (reviewID)
    c.execute("SELECT numWords FROM TABLE2WordCounts WHERE "+str(reviewID)+" IN (reviewID)")
    numWords = c.fetchall()
    #print (numWords[0][0])
    numWords = numWords[0][0]
    normalizedWordCount = count/numWords
    transformedWordCount = 2*math.sqrt(normalizedWordCount + 3.0/8.0)
    #print (transformedWordCount)
    #print (wordsFromStep4)
    if bigram in bigramsFromStep4Dict: #check if the word survived from step 4
        #print ("yes")
        c.execute(insert_TransformedUniqueBigramsPerReview, (transRecordID, reviewID, reviewerID, bigram, count, transformedWordCount))
        transRecordID += 1

c.execute("SELECT * FROM TransformedUniqueBigramsPerReview")
result = c.fetchall()
conn.commit()
conn.close()
