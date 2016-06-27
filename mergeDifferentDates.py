# Given files of user data scrapped from Yelp on different dates
# Merge the files so that each row represents one user
# and his/her elite status on each date
# For example, Jan 4: Not Elite, Jan 5: Not Elite, Jan 6: Elite
# Find on what date the user's status changed to Elite 
import csv
import copy
def combineCityEliteFiles(in_filename1, in_filename2, out_filename, newDate):
    with open(in_filename1, 'rU') as in_f1,\
    open(in_filename2, 'rU') as in_f2, open(out_filename, 'wb') as out_f:
        dataReader1 = csv.reader(in_f1)
        dataReader2 = csv.reader(in_f2) #errorFile higher priority
        originalList = list(dataReader1)
        errorList = list(dataReader2)
        dataWriter = csv.writer(out_f)
        indexUserURL = -5
        indexEliteFlag = -6
        errorDict = {}
        errorLineDict = {} 
        for errorLine in errorList:
            userURL = errorLine[indexUserURL]
            eliteFlag = errorLine[indexEliteFlag] 
            errorDict[userURL] = eliteFlag
            errorLineDict[userURL] = errorLine
        
        copyErrorDict = copy.deepcopy(errorDict)

        for line1 in originalList:
            userURL = line1[indexUserURL]
            newEliteFlag = errorDict.get(userURL, None)
            if "property" in line1[indexUserURL]: 
                dataWriter.writerow([newDate] + line1)
                continue
            if newEliteFlag == None:
                eliteFlagMissing = "NoData"
                dataWriter.writerow([eliteFlagMissing]+line1)
            else:
                dataWriter.writerow([newEliteFlag]+ line1)
                copyErrorDict.pop(userURL, None)

        if len(copyErrorDict)!=0:
            numberOfEntries = len(originalList[0])
            numberOfOthers = 5
            numberOfEliteDataMissing = numberOfEntries-numberOfOthers
            for errorUserURL in copyErrorDict.keys():
                errorLine = errorLineDict[errorUserURL]
                errorEliteFlag = errorDict[errorUserURL]
                if "property" in errorLine[indexUserURL]:
                    continue
                newLine = [errorEliteFlag]+['Missing']*numberOfEliteDataMissing+\
                      errorLine[-5:]
                dataWriter.writerow(newLine)
    return

def main():
    combineCityEliteFiles("mergeJan91011Jan12.csv","Jan8/EliteFlag/cityLabelCombinedEliteFlagJan 8 - Tampa to Cleveland.csv", "mergeJan891011Jan12.csv","Jan8")    
    combineCityEliteFiles("mergeJan891011Jan12.csv","Jan5/EliteFlag/cityLabelCombinedEliteFlagJan 5 - Tampa to Cleveland.csv", "mergeJan5891011Jan12.csv","Jan5")
    combineCityEliteFiles("mergeJan5891011Jan12.csv","Jan4/EliteFlag/cityLabelCombinedEliteFlagJan 4 - Tampa to Cleveland.csv", "mergeJan45891011Jan12.csv","Jan4") 

if __name__ == "__main__":
    main()                
            
