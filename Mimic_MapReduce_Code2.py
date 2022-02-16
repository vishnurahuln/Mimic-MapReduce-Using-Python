import csv #to write output to csv
import pandas as pd

def datacleaningfunc(input_file):
    with open(input_file) as file:  #read the file into a variable
        input_data = file.readlines()

    input_data_cleaned = [] 
    for line in input_data: #iterate through all the lines in the input data
        line = line.replace("\n", "").replace(")", "")\
            .replace("(", "").replace(" ", "") #remove new line special character, paranthesis and spaces.
        line = line.split(",") #spliting based on the delimiter of the file - ',' 
        for i in range(4): #since each line has 4 values, we iterate through each one of them 
            if len(line[i]) == 6:  #to avoid reading the max temp, since YYYYMM has 6 characters.
                year_split = line[i]
                line[i]  = year_split[:-2] #take only the year portion of the YYYYMM
        input_data_cleaned.append(line) #add the value onto the list

    input_data_cleaned = [[int(i) for i in line] for line in input_data_cleaned] #change to integer
    
    return input_data_cleaned


def datasplitter(input_cleaned, partition=0.5): #split the dataset into 2 data sets
    split =  int(len(input_cleaned) * partition) #splits data based on the partition parameter (default to half) and convert to integer type
    return input_cleaned[split:], input_cleaned[:split]

def mapperfunc(input_split):
    combined = []
    
    for i in range(len(input_split)):
        part1, part2 = input_split[i][0:2], input_split[i][2:4] #here we split input into two parts.
        combined.append(part1) #append to list
        combined.append(part2) #append to list
    
    return combined
    
def sortdata(data_mapped):
    return sorted(data_mapped) #sorts by key value pairs

def partition(sorted_data):
  reducer_list1 = [] #declaring the reducer lists for years between 2010 - 2015
  reducer_list2 = [] #declaring the reducer lists for years between 2016 - 2020
  
  for i in range(len(sorted_data)): # All the months in year 2010 to 2015 are sent to Reducer1, and the others (2016 to 2020) are sent to Reducer2
    if sorted_data[i][0] >= 2010 and sorted_data[i][0] <= 2015:
      reducer_list1.append(sorted_data[i])
    else:
      reducer_list2.append(sorted_data[i])
  
  return reducer_list1,reducer_list2

def reducerfunc(partition_result):
    unique_year_temp = dict([]) #stores all unique key,value pairs

    for pairs in partition_result:
        key = pairs[0]
        val = pairs[1]
        if key not in unique_year_temp:
            unique_year_temp[key] = []  #if not found then create an empty list, the value would be appended in the next line
        unique_year_temp[key].append(val)

    for key in unique_year_temp.keys():
        unique_year_temp[key] = max(unique_year_temp[key])
    
    return unique_year_temp

#Main Function
def main():
    input_file = "/Users/vishnurahul/Desktop/Big Data/HW1/temperatures.txt" #store the input file to a variable
    
    input_cleaned = datacleaningfunc(input_file) #data cleaning function

    input_split = datasplitter(input_cleaned) #splitter function
    
    data_mapped1 = mapperfunc(input_split[0])  #mapper function
    data_mapped2 = mapperfunc(input_split[1])  #mapper function
    
    sorted_data1 = sortdata(data_mapped1) #sorting function
    sorted_data2 = sortdata(data_mapped2) #sorting function

    partition_data1 = partition(sorted_data1) #partition function
    partition_data2 = partition(sorted_data2) #partition function
    
    reducer1 = reducerfunc(partition_data1[0]+partition_data2[0]) #reducer function for dates between 2010 to 2015
    reducer2 = reducerfunc(partition_data1[1]+partition_data2[1]) #reducer function for dates between 2016 to 2020
    result = {**reducer1, **reducer2} #combining the results

    #convert the reducer1 into a dataframe using panda's since pandas makes our work easier.
    result_dataframe = pd.DataFrame(list(result.items()),columns = ['Year','Max temp']) 

    #convert the dataframe into a csv file, we keep header = TRUE since our DF already has the columns.
    result_dataframe.to_csv ('/Users/vishnurahul/Desktop/Big Data/HW1/maxTemperature.csv', index = False, header=True)

if __name__ == '__main__':
	main()
