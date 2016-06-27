# Given rows of user data,
# Label the user whose status has changed from Non-elite to Elite 
import csv
def filter_lines(in_filename, out_filename):
    """Read records from in_filename and write records to out_filename 
    """
    eliteFlag = "Elite"
    notEliteFlag = "NotElite" 
    with open(in_filename, 'rU') as in_f, open(out_filename, 'wb') as out_f:
        dataReader = csv.reader(in_f)
        dataWriter = csv.writer(out_f) 
        prev_line = [] # initiate as empty string
        count =0
        for line in dataReader:
            if len(line)!=0:
                if "property" in line[0]:
                    dataWriter.writerow(["EliteFlag"]+ line) 
                elif ("Elite" in line[1]): #the previous index user was elite
                    dataWriter.writerow([eliteFlag]+prev_line)
                elif ("Elite" not in prev_line[1] and "property" not in prev_line[1]):
                    dataWriter.writerow([notEliteFlag]+prev_line)
                prev_line = line
    return 

def main():
    
    filter_lines("Jan12/Jan 12 - Tampa to Cleveland.csv", "EliteFlagJan 12 - Tampa to Cleveland.csv")
    

if __name__ == "__main__":
    main()
