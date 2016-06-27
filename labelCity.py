# Input: rows of user data,
#        dictionary that maps URLs of yelp city managers to the name of their city
# Output: rows of user data labeled with their location 
import csv
def labelCity(dictURLToCityFile, in_filename, out_filename):
    with open(dictURLToCityFile, 'rU') as dict_f, open(in_filename, 'rU') as in_f, open(out_filename, 'wb') as out_f:
        dictReader = csv.reader(dict_f)
        dataReader = csv.reader(in_f)
        dataWriter = csv.writer(out_f)
        dictManagerIDToCity = {} 
        for line in dictReader:
            url = line[0]
            city = line[1] 
            startIndex = url.find("userid=")
            endIndex = url.find("&start=")
            userIDCharNum = 7
            managerID = url[startIndex+userIDCharNum: endIndex]
            dictManagerIDToCity[managerID] = city 
        assert len(dictManagerIDToCity.keys()) == 50
        print dictManagerIDToCity

        for line in dataReader:
            if len(line)!=0 and len(line[4])!=0:
                if "url" in line[4]:
                    dataWriter.writerow(line+["City"])
                    continue
                urlScrappedFrom = line[4]
                startIndex = urlScrappedFrom.find("userid=")
                endIndex = urlScrappedFrom.find("&start=")
                userIDCharNum = 7
                managerID = urlScrappedFrom[startIndex+userIDCharNum: endIndex]
                #print managerID
                if managerID not in dictManagerIDToCity:
                    print "urlScrappedFrom",urlScrappedFrom
                    print "line",line 
                    raise ValueError('manager ID is not in the dictionary'+managerID)
                userLocation = dictManagerIDToCity[managerID]
                dataWriter.writerow(line+[userLocation])
        
                
        return
def main():
    dates = [4, 5, 8, 9, 10, 11, 12]
    for date in dates:
        date = str(date)
        labelCity("urlToLocations.csv",\
                  "Jan"+date+"/EliteFlag/combinedEliteFlagJan "+date+" - Tampa to Cleveland.csv",\
                  "Jan"+date+"/EliteFlag/cityLabelCombinedEliteFlagJan "+date+" - Tampa to Cleveland.csv")
    

if __name__ == "__main__":
    main()
