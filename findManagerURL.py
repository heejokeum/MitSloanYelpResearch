# Given rows of user data scrapped from Yelp,
# Find URLs of community managers' profile 
import csv
def findManagerURL(in_filename, out_filename):
    """Read records from in_filename and write records to out_filename 
    """
    with open(in_filename, 'rU') as in_f, open(out_filename, 'wb') as out_f:
        dataReader = csv.reader(in_f)
        dataWriter = csv.writer(out_f)
        prev_line = [] # initiate as empty string
        count =0
        community_managers = []
        prev_line = ['', '', '', '', '']
        for line in dataReader:
            if len(line)!=0:
                urlScrappedFrom = line[4]
                prevUrlScrappedFrom = prev_line[4]
                ##print urlScrappedFrom
                #print prevUrlScrappedFrom
                if "property" in line[0]:
                    continue
                if "start=0" == urlScrappedFrom[-7:] \
                   and urlScrappedFrom !=  prevUrlScrappedFrom:
                    community_managers.append(urlScrappedFrom)
                    print urlScrappedFrom
                    dataWriter.writerow([urlScrappedFrom,])
                    #dataWriter.writerow(urlScrappedFrom)
                prev_line = line
        

    
    return community_managers

def main():
    managersList2 = findManagerURL("Jan5/EliteFlag/combinedEliteFlagJan 5 - Tampa to Cleveland.csv", "Jan 5 - Community Managers URLs.csv")
    
if __name__ == "__main__":
    main()
