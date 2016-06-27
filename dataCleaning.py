import csv
fileName = 'Feb17/Feb 17 - Tampa to Cleveland Part 1 errors.csv'
with open(fileName, 'rU') as f:
    reader = csv.reader(f)
    your_list = list(reader)

#print your_list, len (your_list) 
# get rid of the first line if it doesn't start with a user 
if "property" in your_list[0][0]:
    your_list = your_list[1:]
print your_list
# your_list has (URL_unique_to_user, username, index, url_scrapped_from)
# url_scrapped_from leads to community manager of region

community_managers = []
# edge case: the first url_scrapped_from 
first_community_manager_url = your_list[0][3]
if "start=0" == first_community_manager_url[-7:]:
    community_managers.append(first_community_manager_url)
    
# for the rest of the file:
# the user is from a different city
# if community manager url is different from the previous index user 
for i in xrange(1, len(your_list)-1):
    previous_user = your_list[i-1]
    current_user = your_list[i]
    prev_community_manager_url= previous_user[3]
    community_manager_url= current_user[3]
    
    if  prev_community_manager_url==community_manager_url:
        continue
    elif "start=0" == community_manager_url[-7:]:
        community_managers.append(community_manager_url)
        




        

### match city with their community manager url 
##from bs4 import BeautifulSoup, SoupStrainer
##import re
##import urllib2
##
##managerURL = community_managers[0]
##doc = urllib2.urlopen(managerURL).read()
##print doc

##for managerURL in community_managers: 
##    doc = urllib2.urlopen(managerURL).read()
##    links = SoupStrainer('a', href=re.compile(r'^test'))
##    soup = [str(elm) for elm in BeautifulSoup(doc, parseOnlyThese=links)]
##    for elm in soup:
##        print elm 


# find elites 
# if username contains "Elite", the previous index user is an elite
listEliteFlags = []
# add a flag to each user if user is an elite add "Elite"
# else, add "NotElite"
# start from the end of the list
eliteFlag = "Elite"
notEliteFlag = "NotElite" 
for i in xrange(len(your_list)-1):
    current_user = your_list[i] 
    username = your_list[i][1]
    nextUsername = your_list[i+1][1] 
    if "Elite" in username:
        continue
    elif "Elite" in nextUsername:
        listEliteFlags.append([eliteFlag]+[username]) #current_user)
    else:
        listEliteFlags.append([notEliteFlag]+[username]) #current_user) 
        


outputList = listEliteFlags
outputFileName = 'EliteFlagFeb 17 - Tampa to Cleveland Part 1 errors.csv'
with open(outputFileName, "wb") as f:
    writer = csv.writer(f)
    writer.writerows(outputList)

