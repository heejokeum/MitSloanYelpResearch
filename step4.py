# Yelp DLA project step 4
# Created by Heejo Clare Keum
import nltk
import nltk.corpus
from nltk.collocations import *
import mysql.connector
from mysql.connector import errorcode
import collections
# Step4.1 is already completed from Step 2
# Please look at step2.py
# Step4.1: counts the number of times a given word or phrase appears for each review file
# Step4.1.1: AllUniqueWords
# for each row of review text data
#   for each word or phrase
#       count number of occurrences
#       save count to the database
conn = mysql.connector.connect(user = "jeffreyklee", password = "cookies123", \
host = "jeffreyklee.mysql.pythonanywhere-services.com", database = "jeffreyklee$yelpData")
c = conn.cursor()

#===============================================================================
# Step 4.2: determine the number of Yelp reviewers using each word or phrse
# Count the number of unique Yelp reviewers in the dataset = ReviewerCount
# Count NumReviewersUsingWord
# Determine boolean UseWord for each user
# for each word/phrase in AllUniqueWords
#   count how many reviewers use the word
# if NumReviewersUsingWord/ReviewerCount > percentCutoff: UseWord = True
percentCutoff = 0.01

#===============================================================================
#Step 4.2.1: for AllUniqueWords
c.execute("SELECT * FROM AllTokens")
allTokensRows = c.fetchall()
c.execute("SELECT * FROM UniqueTokensPerReview")
uniqueTokensPerReview = c.fetchall()
c.execute("SELECT * FROM AllUniqueWords") # I don't think I need this
allUniqueWords = c.fetchall() # I don't think I need this

# create a new table to store numReviewers who use the word
# and a boolean (whether more than 1% of users use the word)
c.execute("DROP TABLE IF EXISTS NumReviewersAllUniqueWords")
create_NumReviewersAllUniqueWords = "CREATE TABLE NumReviewersAllUniqueWords(" \
                        "recordID INT NOT NULL, " \
                        "token VARCHAR(255), " \
                        "numReviewers INT NOT NULL, "\
                        "useWord BOOLEAN, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_NumReviewersAllUniqueWords)
insert_NumReviewersAllUniqueWords = "INSERT INTO NumReviewersAllUniqueWords " \
    "(recordID, token, numReviewers, useWord) VALUES " \
    "(%s, %s, %s, %s)"

# IS NOT USED because of [ \, ", '] these symbols create errors in sql query
# for scalability, count how many reviewers use the word from UniqueTokensPerReview

selectCountUniqueToken = 'SELECT COUNT(*) FROM UniqueTokensPerReview WHERE token = "%s"'

#Initialize numReviewersUsingWordDict
# (key, value) = (word, number of reviewers using word)
numReviewersUsingWordDict = collections.OrderedDict()
for row in allUniqueWords:
    uniqueWord = row[1]
    numReviewersUsingWordDict[uniqueWord] = 0
#Fill in numReviewersUsingWordDict
for row in uniqueTokensPerReview:
    token = row[2]
    try:
        numReviewersUsingWordDict[token] += 1
    except:
        print (token)

#Find how many reviewers in total data
c.execute("SELECT COUNT(*) FROM TABLE2")
totalReviewers = (c.fetchall()[0][0])


#Insert numReviewers and boolean to the new table NumReviewersAllUniqueWords
recordID = 0
for word in numReviewersUsingWordDict:
    recordID +=1
    numReviewers = numReviewersUsingWordDict[word]
    #print (numReviewers, totalReviewers)
    if numReviewers/totalReviewers > percentCutoff:
        c.execute(insert_NumReviewersAllUniqueWords, (recordID, word, numReviewers, True))
    else:
        c.execute(insert_NumReviewersAllUniqueWords, (recordID, word, numReviewers, False))

# test if the output is correct
# since the dataset has only 50 reviewers, all the tokens are marked as True
#c.execute("SELECT * from NumReviewersAllUniqueWords")
#print (c.fetchall())

#===============================================================================
#Step 4.2.2: for Bigrams


# First fetch all bigrams and make them into a dictionary (saves time)
c.execute("SELECT phrase from AllBigramsFinalized")
allBigramsList = c.fetchall()
allBigramsDict = {}
for bigram in allBigramsList:
    stringBigram = str(bigram[0])
    #print (str(bigram[0]), "bigram")
    allBigramsDict[stringBigram] = 0 # place holder


#first create uniqueBigramsPerReview
#Step2.3: create UniqueTokensPerReview table
c.execute("DROP TABLE IF EXISTS `UniqueBigramsPerReview`")
create_UniqueBigramsPerReview = "CREATE TABLE UniqueBigramsPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "bigram VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "PRIMARY KEY (`recordID`))"
insert_UniqueBigramsPerReview = "INSERT INTO UniqueBigramsPerReview "\
    "(recordID, reviewerID, bigram, count) VALUES "\
    "(%s, %s, %s, %s)"


c.execute(create_UniqueBigramsPerReview)
c.execute("SELECT * FROM TABLE2")
rows = c.fetchall()


# Helper function for spliting a review into pairs of words
# "the weather is great today" --> ["the weather", "weather is", "is great", "great today"]
# params:
#   review - review that needs to be split
#   size - how many words in a pair
def splitReviewIntoPair(review, size):
    splitReviewWords = review.split() # list of words
    output = []
    numWords = len(splitReviewWords)
    for i in range(numWords-size+1):
        element = []
        for j in range(size):
            element.append(splitReviewWords[i+j])
        output.append(" ".join(element))
    return output

bigramPerReviewID = 0
for eachRow in rows:
    #print ("=========================================================")
    #print (eachRow)
    reviewerID = eachRow[0]
    #print (reviewerID)
    numStars= eachRow[1]
    reviewText = eachRow[2]
    isElite = eachRow[3]
    splitReviewText = splitReviewIntoPair(reviewText, 2)

    bigramsPerReview = {}
    for phrase in splitReviewText:
        if phrase in allBigramsDict: # first test if it's a valid bigram
            if phrase in bigramsPerReview:
                bigramsPerReview[phrase] += 1
            else:
                bigramsPerReview[phrase] = 1
    for phrase in bigramsPerReview:
        try:
            bigramPerReviewID += 1
            c.execute(insert_UniqueBigramsPerReview, (bigramPerReviewID, reviewerID, phrase, bigramsPerReview[phrase]))
        except mysql.connector.errors.DataError as e:
            print (bigramPerReviewID, bigram)
###############################################################################
# Find bigrams used by less than 1% of the reviewers
#Initialize numReviewersUsingBigramDict
# (key, value) = (bigram, number of reviewers using bigram)
numReviewersUsingBigramDict = collections.OrderedDict()
c.execute("SELECT * FROM UniqueBigramsPerReview")
uniqueBigramsPerReview = c.fetchall()

#for row in allUniqueWords:
#    uniqueWord = row[1]
#    numReviewersUsingWordDict[uniqueWord] = 0

#Fill in numReviewersUsingWordDict
numReviewersUsingBigramDict={}
for row in uniqueBigramsPerReview:
    bigram = row[2]
    try:
        numReviewersUsingBigramDict[bigram] += 1
    except:
        numReviewersUsingBigramDict[bigram] = 1
        print (bigram)

#Find how many reviewers in total data
c.execute("SELECT COUNT(*) FROM TABLE2")
totalReviewers = (c.fetchall()[0][0])

# create a new table to store numReviewers who use the bigram
# and a boolean (whether more than 1% of users use the bigram)
c.execute("DROP TABLE IF EXISTS NumReviewersAllUniqueBigrams")
create_NumReviewersAllUniqueBigrams = "CREATE TABLE NumReviewersAllUniqueBigrams(" \
                        "recordID INT NOT NULL, " \
                        "bigram VARCHAR(255), " \
                        "numReviewers INT NOT NULL, "\
                        "useBigram BOOLEAN, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_NumReviewersAllUniqueBigrams)
insert_NumReviewersAllUniqueBigrams = "INSERT INTO NumReviewersAllUniqueBigrams " \
    "(recordID, bigram, numReviewers, useBigram) VALUES " \
    "(%s, %s, %s, %s)"
#Insert numReviewers and boolean to the new table NumReviewersAllUniqueWords
recordID = 0
#print (numReviewersUsingBigramDict)
for bigram in numReviewersUsingBigramDict:
    recordID +=1
    numReviewers = numReviewersUsingBigramDict[bigram]
    #print (numReviewers, totalReviewers)
    if numReviewers/totalReviewers > percentCutoff:
        c.execute(insert_NumReviewersAllUniqueBigrams, (recordID, bigram, numReviewers, True))
    else:
        c.execute(insert_NumReviewersAllUniqueBigrams, (recordID, bigram, numReviewers, False))

#===============================================================================
#For Trigrams
# First fetch all Trigrams and make them into a dictionary (saves time)
c.execute("SELECT phrase from AllTrigramsFinalized")
allTrigramsList = c.fetchall()
allTrigramsDict = {}
for trigram in allTrigramsList:
    stringTrigram = str(trigram[0])
    allTrigramsDict[stringTrigram] = 0 # place holder

#first create uniqueTrigramsPerReview
#Step2.3: create UniqueTokensPerReview table
c.execute("DROP TABLE IF EXISTS `UniqueTrigramsPerReview`")
create_UniqueTrigramsPerReview = "CREATE TABLE UniqueTrigramsPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "trigram VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "PRIMARY KEY (`recordID`))"
insert_UniqueTrigramsPerReview = "INSERT INTO UniqueTrigramsPerReview "\
    "(recordID, reviewerID, trigram, count) VALUES "\
    "(%s, %s, %s, %s)"


c.execute(create_UniqueTrigramsPerReview)
c.execute("SELECT * FROM TABLE2")
rows = c.fetchall()

trigramPerReviewID = 0
for eachRow in rows:
    #print ("=========================================================")
    #print (eachRow)
    reviewerID = eachRow[0]
    #print (reviewerID)
    numStars= eachRow[1]
    reviewText = eachRow[2]
    isElite = eachRow[3]
    splitReviewText = splitReviewIntoPair(reviewText, 3)

    trigramsPerReview = {}
    for phrase in splitReviewText:
        if phrase in allTrigramsDict: # first test if it's a valid trigram
            if phrase in trigramsPerReview:
                trigramsPerReview[phrase] += 1
            else:
                trigramsPerReview[phrase] = 1
    for phrase in trigramsPerReview:
        try:
            trigramPerReviewID += 1
            c.execute(insert_UniqueTrigramsPerReview, (trigramPerReviewID, reviewerID, phrase, trigramsPerReview[phrase]))
        except mysql.connector.errors.DataError as e:
            print (trigramPerReviewID, trigram)
###############################################################################
# Find trigrams used by less than 1% of the reviewers
#Initialize numReviewersUsingTrigramDict
# (key, value) = (trigram, number of reviewers using trigram)
numReviewersUsingTrigramDict = collections.OrderedDict()
c.execute("SELECT * FROM UniqueTrigramsPerReview")
uniqueTrigramsPerReview = c.fetchall()

#for row in allUniqueWords:
#    uniqueWord = row[1]
#    numReviewersUsingWordDict[uniqueWord] = 0

#Fill in numReviewersUsingWordDict
numReviewersUsingTrigramDict={}
for row in uniqueTrigramsPerReview:
    trigram = row[2]
    try:
        numReviewersUsingTrigramDict[trigram] += 1
    except:
        numReviewersUsingTrigramDict[trigram] = 1
        print (trigram)


# create a new table to store numReviewers who use the trigram
# and a boolean (whether more than 1% of users use the trigram)
c.execute("DROP TABLE IF EXISTS NumReviewersAllUniqueTrigrams")
create_NumReviewersAllUniqueTrigrams = "CREATE TABLE NumReviewersAllUniqueTrigrams(" \
                        "recordID INT NOT NULL, " \
                        "trigram VARCHAR(255), " \
                        "numReviewers INT NOT NULL, "\
                        "useTrigram BOOLEAN, " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_NumReviewersAllUniqueTrigrams)
insert_NumReviewersAllUniqueTrigrams = "INSERT INTO NumReviewersAllUniqueTrigrams " \
    "(recordID, trigram, numReviewers, useTrigram) VALUES " \
    "(%s, %s, %s, %s)"
#Insert numReviewers and boolean to the new table NumReviewersAllUniqueWords
recordID = 0
#print (numReviewersUsingTrigramDict)
for trigram in numReviewersUsingTrigramDict:
    recordID +=1
    numReviewers = numReviewersUsingTrigramDict[trigram]
    #print (numReviewers, totalReviewers)
    if numReviewers/totalReviewers > percentCutoff:
        c.execute(insert_NumReviewersAllUniqueTrigrams, (recordID, trigram, numReviewers, True))
    else:
        c.execute(insert_NumReviewersAllUniqueTrigrams, (recordID, trigram, numReviewers, False))

c.execute("SELECT * FROM NumReviewersAllUniqueTrigrams")
rows= c.fetchall()
conn.commit()
conn.close()





