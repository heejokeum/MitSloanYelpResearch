# Given rows of user data,
# Find people who became friends with Yelp community manager
# In this project, we assume that when a user's status changes to Elite,
# he/she becomes friends with Yelp community manager 
import csv
# find people whose status changed from null to elite 
def findNewElite(in_filename, out_Elite, out_NonElite, out_Change, out_NewFriends):
    with open(in_filename, 'rU') as in_f, open(out_Elite, 'wb') as out_f,\
         open(out_NonElite, 'wb') as out_f_Non,\
         open(out_Change, 'wb') as out_f_Change,\
         open(out_NewFriends, 'wb') as out_f_New_Friends: 

        dataReader = csv.reader(in_f)
        dataWriter = csv.writer(out_f)
        dataWriterNon = csv.writer(out_f_Non)
        dataWriterChange = csv.writer(out_f_Change)
        dataWriterNewFriends = csv.writer(out_f_New_Friends) 
        numOfDates =7

        newElites = []
        newNonElites = []
        newFriendsOfManagerElites = []
        
        dictNewElites = {}

        for line in dataReader:
            listOfFlags = [] # for finding NoData to Elite 
            if len(line)!=0 and "Elite" in line[0]: 
                eliteFlags = line[:numOfDates]
                flag = eliteFlags[0]
                userURL = line[numOfDates]
                for newFlag in eliteFlags[1:]:                   
                    listOfFlags.append(flag) 
                    if flag != newFlag:
                        if flag == "NotElite" and newFlag == "Elite":
                            newElites.append(userURL)
                            dictNewElites[userURL] = ""
                        if flag == "Elite" and newFlag == "NotElite":
                            newNonElites.append(userURL)
                        
                        if flag == "NoData" and newFlag == "Elite":
 
                            if "Elite" not in listOfFlags or "NotElite" not in listOfFlags: 
                                newFriendsOfManagerElites.append(userURL)
                    
                    flag = newFlag
        # check for people who became friends with manager
##        if listOfFlags[-1] == "Elite":
##            if set(listOfFlags) == set(["NoData", "Elite"]):
##                dataWriterNewFriends.writerow([user
##        
        if len(set(newElites)) != len(newElites):
            print "Some users changed their status more than once."
        
        if len(set(newNonElites)) != len(newNonElites):
            print "Some users changed their status more than once."
        for item in newElites:
            dataWriter.writerow([item,])
        for item in newNonElites:
            dataWriterNon.writerow([item,])
        for user in newNonElites:
            if user in dictNewElites:
                dataWriterChange.writerow([user,])
        for item in newFriendsOfManagerElites:
            dataWriterNewFriends.writerow([item,])
        return
def main():
    findNewElite("mergeJan4589101112.csv", "newElites.csv", "newNonElites.csv", "statusChangeMultipleTimes.csv", "newFriendsOfManagerElites.csv") 

if __name__ == "__main__":
    main()
