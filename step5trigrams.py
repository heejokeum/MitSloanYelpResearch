# Yelp DLA project step 2, version 2
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


c.execute("SELECT trigram FROM NumReviewersAllUniqueTrigrams;")
trigramsFromStep4 = c.fetchall()
# Dictionary is faster to check if the trigram survived from Step4
trigramsFromStep4Dict = {}
for word in trigramsFromStep4:
    trigram = word[0]
    trigramsFromStep4Dict[trigram] = False # placeholder



c.execute("SELECT * FROM UniqueTrigramsPerReviewWithID")
uniqueTrigramsPerReviewWithID = c.fetchall()

#Step5.2: Save normalized and transformed Trigram count
c.execute("DROP TABLE IF EXISTS `TransformedUniqueTrigramsPerReview`")
create_TransformedUniqueTrigramsPerReview = "CREATE TABLE TransformedUniqueTrigramsPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "trigram VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "transformedWordCount FLOAT, " \
                         "PRIMARY KEY (`recordID`))"
insert_TransformedUniqueTrigramsPerReview = "INSERT INTO TransformedUniqueTrigramsPerReview "\
    "(recordID, reviewID, reviewerID, trigram, count, transformedWordCount) VALUES "\
    "(%s, %s, %s, %s, %s, %s)"
c.execute(create_TransformedUniqueTrigramsPerReview)

transRecordID = 1
for row in uniqueTrigramsPerReviewWithID:
    #print (row)
    recordID, reviewID, reviewerID, trigram, count = row
    #print (reviewID)
    c.execute("SELECT numWords FROM TABLE2WordCounts WHERE "+str(reviewID)+" IN (reviewID)")
    numWords = c.fetchall()
    #print (numWords[0][0])
    numWords = numWords[0][0]
    normalizedWordCount = count/numWords
    transformedWordCount = 2*math.sqrt(normalizedWordCount + 3.0/8.0)
    #print (transformedWordCount)
    #print (wordsFromStep4)
    if trigram in trigramsFromStep4Dict: #check if the word survived from step 4
        #print ("yes")
        c.execute(insert_TransformedUniqueTrigramsPerReview, (transRecordID, reviewID, reviewerID, trigram, count, transformedWordCount))
        transRecordID += 1

c.execute("SELECT * FROM TransformedUniqueTrigramsPerReview")
result = c.fetchall()
conn.commit()
conn.close()
