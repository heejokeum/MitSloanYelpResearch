# Merge two csv files that contain rows of user data
# The two files may contain redundant rows
# Return a merged file that has non-redundant rows of user data
# the usernames are in alphabetical order, and the order is conserved
import csv
def mergeTwoFiles(in_filename1, in_filename2, out_filename):
    """Read records from in_filename and write records to out_filename 
    """
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

            while len(originalList) !=0 and not restart:
                if popLine1: 
                    line1 = originalList.pop(0)
                if len(line1) ==0:
                    continue               
                user = line1[2]
                userURL = line1[1]
                if "property" in line1[1]:
                    dataWriter.writerow(line1)
                    popLine1 = True 
                    continue 
                if userURL == errorUserURL:
                    dataWriter.writerow(errorLine)
                    restart = True
                elif user > errorUser and len(errorUser)!=0: #user=bc, errorUser=abc
                    dataWriter.writerow(errorLine)
                    popLine1 = False
                    restart= True 
                else:
                    dataWriter.writerow(line1)
                    popLine1 = True
    return 

def main():
    mergeTwoFiles("Jan12/EliteFlag/EliteFlagJan 12 - Tampa to Cleveland.csv","Jan12/EliteFlag/EliteFlagJan 12 - Tampa to Cleveland errors.csv", "Jan12/EliteFlag/combinedEliteFlagJan 12 - Tampa to Cleveland.csv")
if __name__ == "__main__":
    main()
