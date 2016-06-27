# Yelp DLA project step 5 for tokenized words
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

# Step5.1: count how many words are in each review
c.execute("SELECT * FROM TABLE2")
#c.execute("SELECT user_id, text FROM TABLE2")
rows = c.fetchall()
#numWordCounts = collections.OrderedDict()
#for row in rows:
#    print (row)
#    user_id, rating, text, eliteCount = row[0], row[1], row[2], row[3]
#    words = text.split()
#    numWordCounts[(user_id, text)] = len(words)
    #print (numWordCounts)
numWordCounts = []
for row in rows:
    user_id, rating, text, eliteCount = row[0], row[1], row[2], row[3]
    words = text.split()
    numWordCounts.append((user_id, rating, text, eliteCount, len(words)))

c.execute("DROP TABLE IF EXISTS TABLE2WordCounts")
# add a revised version of TABLE2 with wordcounts and unique ID for each review
create_TABLE2WordCounts = "CREATE TABLE TABLE2WordCounts(" \
                        "recordID INT NOT NULL, " \
                        "reviewID INT NOT NULL, " \
                        "reviewerID VARCHAR(255), " \
                        "rating INT NOT NULL, "\
                        "text VARCHAR(8000), "\
                        "eliteCount BOOLEAN, "\
                        "numWords INT NOT NULL, "\
                        "PRIMARY KEY (`recordID`))"
c.execute(create_TABLE2WordCounts)
insert_TABLE2WordCounts = "INSERT INTO TABLE2WordCounts " \
    "(recordID, reviewID, reviewerID, rating, text, eliteCount, numWords) VALUES " \
    "(%s, %s, %s, %s, %s, %s, %s)"
recordID = 1
reviewID = 1
for entry in numWordCounts:
    newtuple = (recordID, reviewID, ) + entry
    #print (newtuple)
    c.execute(insert_TABLE2WordCounts, newtuple)
    recordID += 1
    reviewID += 1
c.execute("SELECT * FROM TABLE2WordCounts")
result = c.fetchall()
#print (result)

#===============================================================================
c.execute("SELECT token FROM NumReviewersAllUniqueWords;")
wordsFromStep4 = c.fetchall()
# Dictionary is faster to check if the word survived from Step4
wordsFromStep4Dict = {}
for word in wordsFromStep4:
    #print (word[0])
    token = word[0]
    wordsFromStep4Dict[token] = False # placeholder
c.execute("SELECT * FROM UniqueTokensPerReviewWithID")
uniqueTokensPerReviewWithID = c.fetchall()

#Step5.2: Save normalized and transformed word count
c.execute("DROP TABLE IF EXISTS `TransformedUniqueTokensPerReview`")
create_TransformedUniqueTokensPerReview = "CREATE TABLE TransformedUniqueTokensPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "token VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "transformedWordCount FLOAT, " \
                         "PRIMARY KEY (`recordID`))"
insert_TransformedUniqueTokensPerReview = "INSERT INTO TransformedUniqueTokensPerReview "\
    "(recordID, reviewID, reviewerID, token, count, transformedWordCount) VALUES "\
    "(%s, %s, %s, %s, %s, %s)"
c.execute(create_TransformedUniqueTokensPerReview)

transRecordID = 1
for row in uniqueTokensPerReviewWithID:
    #print (row)
    recordID, reviewID, reviewerID, token, count = row
    #print (reviewID)
    c.execute("SELECT numWords FROM TABLE2WordCounts WHERE "+str(reviewID)+" IN (reviewID)")
    numWords = c.fetchall()
    #print (numWords[0][0])
    numWords = numWords[0][0]
    normalizedWordCount = count/numWords
    transformedWordCount = 2*math.sqrt(normalizedWordCount + 3.0/8.0)
    #print (transformedWordCount)
    #print (wordsFromStep4)
    if token in wordsFromStep4Dict: #check if the word survived from step 4
        #print ("yes")
        c.execute(insert_TransformedUniqueTokensPerReview, (transRecordID, reviewID, reviewerID, token, count, transformedWordCount))
        transRecordID += 1

#===============================================================================
c.execute("SELECT token FROM NumReviewersAllUniqueBigrams;")
wordsFromStep4 = c.fetchall()
# Dictionary is faster to check if the bigram survived from Step4
wordsFromStep4Dict = {}
for word in wordsFromStep4:
    #print (word[0])
    token = word[0]
    wordsFromStep4Dict[token] = False # placeholder
print (wordsFromStep4Dict)
conn.close()
c.execute("SELECT * FROM UniqueBigramsPerReviewWithID")
uniqueBigramsPerReviewWithID = c.fetchall()

#Step5.2: Save normalized and transformed word count
c.execute("DROP TABLE IF EXISTS `TransformedUniqueTokensPerReview`")
create_TransformedUniqueTokensPerReview = "CREATE TABLE TransformedUniqueTokensPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "token VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "transformedWordCount FLOAT, " \
                         "PRIMARY KEY (`recordID`))"
insert_TransformedUniqueTokensPerReview = "INSERT INTO TransformedUniqueTokensPerReview "\
    "(recordID, reviewID, reviewerID, token, count, transformedWordCount) VALUES "\
    "(%s, %s, %s, %s, %s, %s)"
c.execute(create_TransformedUniqueTokensPerReview)

transRecordID = 1
for row in uniqueTokensPerReviewWithID:
    #print (row)
    recordID, reviewID, reviewerID, token, count = row
    #print (reviewID)
    c.execute("SELECT numWords FROM TABLE2WordCounts WHERE "+str(reviewID)+" IN (reviewID)")
    numWords = c.fetchall()
    #print (numWords[0][0])
    numWords = numWords[0][0]
    normalizedWordCount = count/numWords
    transformedWordCount = 2*math.sqrt(normalizedWordCount + 3.0/8.0)
    #print (transformedWordCount)
    #print (wordsFromStep4)
    if token in wordsFromStep4Dict: #check if the word survived from step 4
        #print ("yes")
        c.execute(insert_TransformedUniqueTokensPerReview, (transRecordID, reviewID, reviewerID, token, count, transformedWordCount))
        transRecordID += 1
c.execute("SELECT * FROM TransformedUniqueTokensPerReview")
result = c.fetchall()
#print (result)
conn.commit()
conn.close()
