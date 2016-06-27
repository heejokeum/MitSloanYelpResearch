# Yelp DLA project step 4, version 3
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

################################################################################
#REVISION with Window - Version2 only looks at adjacent words
#first create uniqueBigramsPerReviewWithIDWithWindow
#REVISED Step2.3: create UniqueTokensPerReviewWithID table
#previous version does not have unique ID for each review
#REVISED look at the window, instead of adjacent words
c.execute("DROP TABLE IF EXISTS `UniqueBigramsPerReviewWithIDWithWindow`")
create_UniqueBigramsPerReviewWithIDWithWindow = "CREATE TABLE UniqueBigramsPerReviewWithIDWithWindow(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "bigram VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "PRIMARY KEY (`recordID`))"
insert_UniqueBigramsPerReviewWithIDWithWindow = "INSERT INTO UniqueBigramsPerReviewWithIDWithWindow "\
    "(recordID, reviewID, reviewerID, bigram, count) VALUES "\
    "(%s, %s, %s, %s, %s)"

c.execute(create_UniqueBigramsPerReviewWithIDWithWindow)
c.execute("SELECT * FROM TABLE2")
rows = c.fetchall()

################################################################################
# REVISION - deleted splitReviewIntoPair
# Helper function for spliting a review into a window of words
# "the weather is very nice today" --> [{"the": 1, "weather": 1, "is": 1, "very": 1, "nice": 1}, {"weather": 1, "is": 1, "very": 1, "nice": 1, "today":1}]
# params:
#    review - review that needs to be split
#    windowSize - how many words in a window
def splitReviewIntoWindow(review, windowSize):
    splitReviewWords = review.split() # list of words
    output = [] #list of dictionaries
    numWords = len(splitReviewWords)
    for i in range(numWords-windowSize+1):
        element = {}
        for j in range(windowSize):
            element[splitReviewWords[i+j]] = 1 #placeholder
        #print (element, "element")
        output.append(element)
    return output

#testReview = "the weather is very nice today"
#print (splitReviewIntoWindow(testReview, 5))

bigramPerReviewID = 0
reviewID = 0
for eachRow in rows:
    #print ("=========================================================")
    #print (eachRow)
    reviewID += 1
    reviewerID = eachRow[0]
    #print (reviewerID)
    numStars= eachRow[1]
    reviewText = eachRow[2]
    isElite = eachRow[3]
    splitReviewText = splitReviewIntoWindow(reviewText, 5)
    bigramsPerReview = {}

    for phrase in allBigramsDict:
        bigram = phrase.split()
        #print (bigram)
        word1 = bigram[0]
        word2 = bigram[1]
        for window in splitReviewText:
            if word1 in window and word2 in window: # if the bigram exists in the window
                if phrase in bigramsPerReview:
                    bigramsPerReview[phrase] += 1
                else:
                    bigramsPerReview[phrase] = 1

    for phrase in bigramsPerReview:
        try:
            bigramPerReviewID += 1
            c.execute(insert_UniqueBigramsPerReviewWithIDWithWindow, (bigramPerReviewID, reviewID, reviewerID, phrase, bigramsPerReview[phrase]))
        except mysql.connector.errors.DataError as e:
            print (bigramPerReviewID, bigram)
#c.execute("SELECT * from UniqueBigramsPerReviewWithIDWithWindow")
#result = c.fetchall()
#print (result)
###############################################################################
# REVISION - Version2 only considers adjacent words
#          - change so that we look at the table UniqueBigramsPerReviewWithIDWithWindow
# Find bigrams used by less than 1% of the reviewers
#Initialize numReviewersUsingBigramDict
# (key, value) = (bigram, number of reviewers using bigram)
numReviewersUsingBigramDict = collections.OrderedDict()
c.execute("SELECT * FROM UniqueBigramsPerReviewWithIDWithWindow")
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
        #print (bigram)

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

################################################################################
#REVISION with Window - Version2 only looks at adjacent words
#first create uniqueTrigramsPerReviewWithIDWithWindow
#Step2.3: create UniqueTokensPerReviewWithIDWithWindow table
c.execute("DROP TABLE IF EXISTS `UniqueTrigramsPerReviewWithIDWithWindow`")
create_UniqueTrigramsPerReviewWithIDWithWindow = "CREATE TABLE UniqueTrigramsPerReviewWithIDWithWindow(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "trigram VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "PRIMARY KEY (`recordID`))"
insert_UniqueTrigramsPerReviewWithIDWithWindow = "INSERT INTO UniqueTrigramsPerReviewWithIDWithWindow "\
    "(recordID, reviewID, reviewerID, trigram, count) VALUES "\
    "(%s, %s, %s, %s, %s)"


c.execute(create_UniqueTrigramsPerReviewWithIDWithWindow)
c.execute("SELECT * FROM TABLE2")
rows = c.fetchall()

trigramPerReviewID = 0
reviewID = 0
for eachRow in rows:
    #print ("=========================================================")
    #print (eachRow)
    reviewID += 1
    reviewerID = eachRow[0]
    #print (reviewerID)
    numStars= eachRow[1]
    reviewText = eachRow[2]
    isElite = eachRow[3]
    splitReviewText = splitReviewIntoWindow(reviewText, 5)



    trigramsPerReview = {}
    for phrase in allTrigramsDict:
        trigram = phrase.split()
        #print (trigram)
        word1 = trigram[0]
        word2 = trigram[1]
        word3 = trigram[2]
        for window in splitReviewText:
            if word1 in window and word2 in window and word3 in window: # if the trigram exists in the window
                if phrase in trigramsPerReview:
                    trigramsPerReview[phrase] += 1
                else:
                    trigramsPerReview[phrase] = 1


    for phrase in trigramsPerReview:
        try:
            trigramPerReviewID += 1
            c.execute(insert_UniqueTrigramsPerReviewWithIDWithWindow, (trigramPerReviewID, reviewID, reviewerID, phrase, trigramsPerReview[phrase]))
        except mysql.connector.errors.DataError as e:
            print (trigramPerReviewID, trigram)
c.execute("SELECT * FROM UniqueTrigramsPerReviewWithIDWithWindow")
result = c.fetchall()
#print (result)
###############################################################################
# REVISION - Version2 only considers adjacent words
#          - change so that we look at the table UniqueTrigramsPerReviewWithIDWithWindow
# Find trigrams used by less than 1% of the reviewers
#Initialize numReviewersUsingTrigramDict
# (key, value) = (trigram, number of reviewers using trigram)
numReviewersUsingTrigramDict = collections.OrderedDict()
c.execute("SELECT * FROM UniqueTrigramsPerReviewWithIDWithWindow")
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
        #print (trigram)


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





