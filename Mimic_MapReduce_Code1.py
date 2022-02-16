from posixpath import split
import pandas as pd
import numpy as np

class MapReduce:
    def convert_list_of_int(self, input_data):
        list1=[]
        list2=[]

        cleaned_lines = [i.split('\n')[0].strip("()").split("), (") for i in input_data] #creating a List[List[str]]

        for vals in cleaned_lines: #creating a List[str]
            list1 += vals 
        
        for vals in list1:         #creating a List[int]
            temp = vals.split(", ")
            temp[0]=int(temp[0]) #converting to int
            temp[1]=int(temp[1]) #converting to int
            list2.append(temp)   #append it onto a list

        return list2

    def data_split(self, input_data_integer,split_percentage = 0.5):
        split_val = int(len(input_data_integer)*split_percentage) #creating spliting variable
        return input_data_integer[split_val:], input_data_integer[:split_val]

    def mapper(self, data_split):
        list_ = []
        for i in data_split:
            tmp = int(i[0]/100)     #getting only the year part
            list_.append((tmp, i[1]))
        dict_ = dict([])

        for k,v in list_:           #combiners
            if k in dict_:
                dict_[k].append(v)
            else:
                dict_[k] = [v]
        return dict_

    def sort_func(self,mapped1,mapped2):
        dict_={}
        for i in sorted (mapped1):  #sorts the first mapped input
            dict_[i]=mapped1[i]
        for items in sorted(mapped2): #sorts the second mapped input
            if items in dict_:
                dict_[items].extend(mapped2[items])
            else:
                dict_[items]=mapped2[items]
        return dict_
    
    def partition(self,sorted_data):
        dict1_={}
        dict2_={}
        for i in sorted_data:
            if i in range(2010,2016): #2016 not inclusive, so range is from 2010 to 2015
                dict1_[i]=sorted_data[i]
            else:
                dict2_[i]=sorted_data[i] #rest of the data, 2016 - 2020
        return dict1_,dict2_
    
    def reducer(self,partitioned_data):
        dict1_={}
        for i in partitioned_data:
            dict1_[i]=max(partitioned_data[i])  #finds the max value in the partitioned_data for each year
        return dict1_

def main():
    map_reduce = MapReduce() #creating an object of class MapReduce

    with open('/Users/vishnurahul/Desktop/Big Data/HW1/temperatures.txt') as f: 
        input_data = f.readlines()

    input_data_integer = map_reduce.convert_list_of_int(input_data) #converting string to integer
    data_split1,data_split2 = map_reduce.data_split(input_data_integer,0.5) #spliting the input into two parts and passing splitting parameter to 0.5 (or half)

    mapped1 = map_reduce.mapper(data_split1)
    mapped2 = map_reduce.mapper(data_split2)
    sorted_data = map_reduce.sort_func(mapped1,mapped2)
    partitioned_data1,partitioned_data2 = map_reduce.partition(sorted_data)
    reduced_data1=map_reduce.reducer(partitioned_data1)
    reduced_data2=map_reduce.reducer(partitioned_data2)
    reduced_data1.update(reduced_data2) #combining the two dicts

    data_frame=pd.DataFrame(list(reduced_data1.items()),columns = ['Year','Max temperature']) #convert dict to list to dataframe
    data_frame.to_csv('/Users/vishnurahul/Desktop/Big Data/HW1/op_megha.txt', index = False, header=True) # saving data_frame as a text output file

if __name__ == '__main__':
	main()
