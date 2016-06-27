# Yelp DLA project step 3
# Created by Heejo Clare Keum
#!/usr/bin/env python2.7
import nltk
import nltk.corpus
from nltk.collocations import *
import mysql.connector
from mysql.connector import errorcode

conn = mysql.connector.connect(user = "jeffreyklee", password = "cookies123", \
host = "jeffreyklee.mysql.pythonanywhere-services.com", database = "jeffreyklee$yelpData")
c = conn.cursor()
c.execute("SELECT * FROM AllTokens")
rows = c.fetchall()
filteredTokens = []
stopwords = nltk.corpus.stopwords.words('english')
#the current stopwords do not filter punctuations
#add punctuations to the stopwords list
#addStopwords = ['\\', 'r', '"', "'", ".", "-", ",", "(", ")", "/"]
#stopwords += addStopwords

# eachRow has format (recordID, reviewID, reviewerID, seq, token)
# filter the stopwords
for eachRow in rows:
    token = eachRow[4]
    if token.lower() not in stopwords: # confirm that this works
        filteredTokens.append(token)


# Step3.1.1: Create AllBigrams table
c.execute("DROP TABLE IF EXISTS `AllBigrams`")
create_AllBigrams = "CREATE TABLE AllBigrams(" \
                        "recordID INT NOT NULL, " \
                        "token1 VARCHAR(255), " \
                        "token2 VARCHAR(255), " \
                        "pmiScore FLOAT NOT NULL, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_AllBigrams)
insert_AllBigrams = "INSERT INTO AllBigrams " \
    "(recordID, token1, token2, pmiScore) VALUES " \
    "(%s, %s, %s, %s)"


# Step3.1.2: Create AllBigramsFinalized table
c.execute("DROP TABLE IF EXISTS `AllBigramsFinalized`")
create_AllBigramsFinalized = "CREATE TABLE AllBigramsFinalized(" \
                        "recordID INT NOT NULL, " \
                        "phrase VARCHAR(255), " \
                        "pmiScore FLOAT NOT NULL, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_AllBigramsFinalized)
insert_AllBigramsFinalized = "INSERT INTO AllBigramsFinalized " \
    "(recordID, phrase, pmiScore) VALUES " \
    "(%s, %s, %s)"

# add bigrams to the table
bigram_measures = nltk.collocations.BigramAssocMeasures()
finder = BigramCollocationFinder.from_words(filteredTokens, 5)
'''finder.apply_freq_filter(5)'''
recordID = 0
finalRecordID = 0 #recordID for AllBigramsFinalized
cutoffBigramPMI = 6
for i in finder.score_ngrams(bigram_measures.pmi):
    recordID += 1
    firstWord = i[0][0] # token1
    secondWord = i[0][1] # token2
    pmiScore = i[1]
    phrase = firstWord + " " + secondWord
    #print (phrase)

    # Step3.1.1 AllBigrams table
    try:
        c.execute(insert_AllBigrams, (recordID, firstWord, secondWord, pmiScore))
    except mysql.connector.errors.DataError as e:
        print (recordID, firstWord, secondWord)

    # Step3.1.2 AllBigramsFinalized table
    # check if pmiScore is greater than the cutoff
    if pmiScore > cutoffBigramPMI:
        try:
            finalRecordID += 1
            c.execute(insert_AllBigramsFinalized, (finalRecordID, phrase, pmiScore))
        except mysql.connector.errors.DataError as e:
          print (recordID, firstWord, secondWord)

# Step3.2.1: Create AllTrigrams table
c.execute("DROP TABLE IF EXISTS `AllTrigrams`")
create_AllTrigrams = "CREATE TABLE AllTrigrams(" \
                        "recordID INT NOT NULL, " \
                        "token1 VARCHAR(255), " \
                        "token2 VARCHAR(255), " \
                        "token3 VARCHAR(255), " \
                        "pmiScore FLOAT NOT NULL, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_AllTrigrams)
insert_AllTrigrams = "INSERT INTO AllTrigrams " \
    "(recordID, token1, token2, token3, pmiScore) VALUES " \
    "(%s, %s, %s, %s, %s)"

# Step3.2.2: Create AllTrigramsFinalized table
c.execute("DROP TABLE IF EXISTS `AllTrigramsFinalized`")
create_AllTrigramsFinalized = "CREATE TABLE AllTrigramsFinalized(" \
                        "recordID INT NOT NULL, " \
                        "phrase VARCHAR(255), " \
                        "pmiScore FLOAT NOT NULL, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_AllTrigramsFinalized)
insert_AllTrigramsFinalized = "INSERT INTO AllTrigramsFinalized " \
    "(recordID, phrase, pmiScore) VALUES " \
    "(%s, %s, %s)"

# add trigrams to the table
trigram_measures = nltk.collocations.TrigramAssocMeasures()
finder = TrigramCollocationFinder.from_words(filteredTokens, 5)
'''finder.apply_freq_filter(5)'''
recordID = 0
finalRecordID = 0 # recordID for allTrigramsFinalized
cutoffTrigramPMI = 9
for i in finder.score_ngrams(trigram_measures.pmi):
    recordID += 1
    #print (i)
    firstWord = i[0][0] # token1
    secondWord = i[0][1] # token2
    thirdWord = i[0][2] # token3
    pmiScore = i[1]
    phrase = firstWord + " " + secondWord + " " + thirdWord

    # Step3.2.1
    try:
        c.execute(insert_AllTrigrams, (recordID, firstWord, secondWord, thirdWord, pmiScore))
    except mysql.connector.errors.DataError as e:
        print (recordID, firstWord, secondWord, thirdWord)

    # Step3.2.2
    if pmiScore > cutoffTrigramPMI:
        try:
            finalRecordID += 1
            c.execute(insert_AllTrigramsFinalized, (finalRecordID, phrase, pmiScore))
        except mysql.connector.errors.DataError as e:
            print (recordID, firstWord, secondWord, thirdWord)


conn.commit()
conn.close()


