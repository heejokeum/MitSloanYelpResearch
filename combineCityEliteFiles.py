import csv
def combineCityEliteFiles(in_filename1, in_filename2, out_filename, newDate):
    with open(in_filename1, 'rU') as in_f1,\
    open(in_filename2, 'rU') as in_f2, open(out_filename, 'wb') as out_f:
        dataReader1 = csv.reader(in_f1)
        dataReader2 = csv.reader(in_f2) #errorFile higher priority
        originalList = list(dataReader1)
        errorList = list(dataReader2)
        dataWriter = csv.writer(out_f)
        popLine1 = True
        while len(errorList)!=0 or len(originalList)!=0: 
            restart = False
            if len(errorList) !=0: 
                errorLine = errorList.pop(0)
                errorUser = errorLine[2]
                errorUserURL = errorLine[1]
                if "property" in errorLine[1]:
                    continue 
                if len(errorLine) ==0:
                    continue 
            else:
                errorLine = []
                errorUser = ''
                errorUserURL = ''
            #print errorUser
            while len(originalList) !=0 and not restart:
                if popLine1: 
                    line1 = originalList.pop(0)
                if len(line1) ==0:
                    continue               
                user = line1[-4]
                userURL = line1[-5]
                if "property" in line1[-5]: 
                    popLine1 = True
                    dataWriter.writerow([newDate] + line1)
                    continue 
                if userURL == errorUserURL:
                    errorEliteFlag = errorLine[0]
                    dataWriter.writerow([errorEliteFlag]+ line1)
                    restart = True
                    
                elif user > errorUser and len(errorUser)!=0: #user=bc, errorUser=abc
                    errorEliteFlag = errorLine[0]
                    location = errorLine[5]
                    numberOfEntries = len(line1)
                    numberOfOthers = 5
                    numberOfEliteDataMissing = numberOfEntries-numberOfOthers
                    newLine = [errorEliteFlag]+['Missing']*numberOfEliteDataMissing+\
                              errorLine[-5:]
                    dataWriter.writerow(newLine)
                    popLine1 = False
                    restart= True
 
                else:
                    eliteFlagMissing = "NoData"
                    dataWriter.writerow([eliteFlagMissing]+line1)
                    popLine1 = True
##            if len(errorList) >=0 and len(originalList) ==0: # if there are more users in errorList left
##                errorEliteFlag = errorLine[0]
##                location = errorLine[5]
##                numberOfEntries = len(line1)
##                numberOfEliteDataMissing = numberOfEntries-1-3 
##                newLine = [''] + [errorUserURL, errorUser, location]\
##                          +['']*numberOfEliteDataMissing+[errorEliteFlag]
##                dataWriter.writerow(newLine)
##
                

    return
def main():
    combineCityEliteFiles("mergeJan11Jan12.csv","Jan10/EliteFlag/cityLabelCombinedEliteFlagJan 10 - Tampa to Cleveland.csv", "mergeJan1011Jan12.csv","Jan10") 
    #combineCityEliteFiles("usersFromJan4.csv","Jan4/EliteFlag/cityLabelCombinedEliteFlagJan 4 - Tampa to Cleveland.csv", "filteredCityLabelCombinedEliteFlagJan 4 - Tampa to Cleveland.csv")
if __name__ == "__main__":
    main()

