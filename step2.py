#!/usr/bin/env python2.7
# Yelp DLA project step 2 
# Created by Heejo Clare Keum 
import happyfuntokenizing
import mysql.connector
from mysql.connector import errorcode

conn = mysql.connector.connect(user = "jeffreyklee", password = "cookies123", host = "jeffreyklee.mysql.pythonanywhere-services.com", database = "jeffreyklee$yelpData")


c = conn.cursor()
c.execute("SELECT * FROM TABLE2")
rows = c.fetchall()

#Step2.1: create AllTokens table
c.execute("DROP TABLE IF EXISTS `AllTokens`")
create_AllTokens = "CREATE TABLE AllTokens(" \
                         "recordID INT NOT NULL, " \
                         "reviewID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "seq INT NOT NULL, " \
                         "token VARCHAR(255), " \
                         "PRIMARY KEY (`recordID`))"
c.execute(create_AllTokens)
insert_AllTokens = "INSERT INTO AllTokens "\
    "(recordID, reviewID, reviewerID, seq, token) VALUES "\
    "(%s, %s, %s, %s, %s)"

#Step2.2: create AllUniqueWords table
c.execute("DROP TABLE IF EXISTS `AllUniqueWords`")
create_AllUniqueWords = "CREATE TABLE AllUniqueWords(" \
                        "recordID INT NOT NULL, " \
                        "token VARCHAR(255), " \
                        "PRIMARY KEY (`recordID`))"
c.execute(create_AllUniqueWords)
insert_AllUniqueWords = "INSERT INTO AllUniqueWords " \
    "(recordID, token) VALUES " \
    "(%s, %s)"

#Step2.3: create UniqueTokensPerReview table
c.execute("DROP TABLE IF EXISTS `UniqueTokensPerReview`")
create_UniqueTokensPerReview = "CREATE TABLE UniqueTokensPerReview(" \
                         "recordID INT NOT NULL, " \
                         "reviewerID VARCHAR(255), " \
                         "token VARCHAR(255), " \
                         "count INT NOT NULL, " \
                         "PRIMARY KEY (`recordID`))"
insert_UniqueTokensPerReview = "INSERT INTO UniqueTokensPerReview "\
    "(recordID, reviewerID, token, count) VALUES "\
    "(%s, %s, %s, %s)"
c.execute(create_UniqueTokensPerReview)



#Step2.0: tokenize the data
tok = happyfuntokenizing.Tokenizer(preserve_case=False)
reviewID = 0 #index for all reviews
recordID = 0 #index for all tokens
uniqueTokens = {} #Step2.2
uniqueTokenRecordID = 0 #Step2.3
tokenPerReviewID = 0 #Step2.3
for eachRow in rows:
    reviewID += 1
    print "======================================================================"
    #print eachRow
    reviewerID = eachRow[0]
    numStars= eachRow[1]
    reviewText = eachRow[2]
    isElite = eachRow[3]
    tokenized = tok.tokenize(eachRow)

    seq = 0 #index for tokens in one review
    for token in tokenized:
        recordID += 1
        seq += 1

        #Step2.1: insert into AllTokens
        try:
            c.execute(insert_AllTokens, (recordID, reviewID, reviewerID, seq, token))
        except mysql.connector.errors.DataError as e:
            print reviewerID, token

        #Step2.2: insert into AllUniqueWords
        try:
            if token not in uniqueTokens:
                uniqueTokenRecordID += 1
                c.execute(insert_AllUniqueWords, (uniqueTokenRecordID, token))
                uniqueTokens[token] = True; #placeholding
        except mysql.connector.errors.DataError as e:
            print uniqueTokenRecordID, token

    #Step2.3: insert into UniqueTokensPerReview
    tokensPerReview = {}
    for token in tokenized:
        if token in tokensPerReview:
            tokensPerReview[token] += 1
        else:
            tokensPerReview[token] = 1

    for token in tokensPerReview:
        try:
            tokenPerReviewID += 1
            c.execute(insert_UniqueTokensPerReview, (tokenPerReviewID, reviewerID, token, tokensPerReview[token]))
        except mysql.connector.errors.DataError as e:
            print tokenPerReviewID, token


conn.commit()
conn.close()
