# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd 
import numpy as np 
from sklearn import linear_model
from sklearn.preprocessing import PolynomialFeatures
from sklearn.feature_selection import chi2

#create dataframes by reading csv files 
dfMaster = pd.read_csv('./master8-19-16.csv')
dfTransformed = pd.read_csv('./transformeduniquetokensperreview8-19-16.csv')
dfElite = dfMaster[["user_id", "was_elite"]] #extract elite variable 
# note that chain variable was not given in csv, it should be added

# transformedUniqueTokensPerReviewFile didn't explictly state which reviewer is elite
# create a mapping to do so 
reviewerIDToElite = {} #maps reviewerID to elite variable 
for row in dfElite.itertuples(): 
    reviewerID = row[1] 
    isElite = row[2]
    if reviewerID in reviewerIDToElite: 
        assert(reviewerIDToElite[reviewerID] == isElite), "isElite different for same user" 
    else: 
        reviewerIDToElite[reviewerID] = isElite 

# dfTransformed contains info unnecessary for linear regression 
trimmedDfTransformed = dfTransformed[["reviewerID", "token", "transformedWordCount"]]
# should substitute reviewerID with elite variable but cannot iterate and modify the same dataframe 
copyTrimmedDfTransformed = trimmedDfTransformed.copy() 

# substitute reviewerID with elite variable (whether the reviewer is elite)
for row in trimmedDfTransformed.itertuples(): 
    index = row[0]
    reviewerID = row[1]
    isElite = reviewerIDToElite[reviewerID]
    copyTrimmedDfTransformed.set_value(index, "reviewerID", isElite)
copyTrimmedDfTransformed.rename(columns={'reviewerID':'isElite'}, inplace=True) # end of preprocessing the dataframe 

# now apply linear regression
processedDf = copyTrimmedDfTransformed # structure: isElite, token, transformedWordCount
#find the non-redundant set of tokens
dfTokens = dfTransformed["token"] 
setTokens = set(dfTokens) #number of tokens = 1296
testSetTokens = ["this", "your"] 

regressionResults = {} #key: value = token: regression coeffients, regression intercept 
for token in setTokens: #testSetTokens: 
    #isElite, transformedCount for each token
    inputRegression = (processedDf.loc[processedDf['token'] == token]) 
    regr = linear_model.LinearRegression()
    regr.fit(inputRegression["isElite"].reshape(len(inputRegression["isElite"]), 1), inputRegression["transformedWordCount"].transpose())
    regressionResults[token] = regr.coef_, regr.intercept_
dfRegressionResults = pd.DataFrame(regressionResults).transpose() #put it into dataframe 
dfRegressionResults.to_csv(path_or_buf="./regressionResults.csv" )
    
    

    
    

