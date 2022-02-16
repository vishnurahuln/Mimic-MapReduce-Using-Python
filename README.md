# Mimic-MapReduce-Using-Python
Mimic the process of the MapReduce task. Own map and reduce functions to mimic the process of mapper and reducer.


Dataset
The input of this homework is a text document (1000 lines) which includes 2000 pairs of Year/month and average temperature of that month. It is raw data. You need to do some data cleaning work to prepare it for the next step.

Task
You are supposed to build several functions to mimic each step of MapReduce. They are:

-> Data cleaning function:	Some data cleaning jobs, such as removing punctuations and special symbols, converting string to integer (you need to do that to calculate the max.), etc.

-> Data split function:	Split the dataset into two parts: Part1 includes the first 500 lines of the Raw data, Part2 includes the rest 500 lines.	

-> Mapper function: Two mapper functions that produce a set of key-value pairs for Part1 and Part2 subsets respectively. 	

-> Sort function: Sort by key of Part1 and Part2 together, with an ascending sort order.

-> Partition function:	All the months in year 2010 to 2015 are sent to Reducer1, and the others (2016 to 2020) are sent to Reducer2.	

-> Reducer function: Collect all values belonging to the key and find the maximum temperature for the two ordered partitions.	

-> Main function:	Wrap all the steps together and combine the output of the two partitions together.	

The figure below shows the basic workflow of this word count task.

<img width="468" alt="image" src="https://user-images.githubusercontent.com/89628033/154361988-9c71ce0a-ef25-48b4-8d6a-126ec41f5185.png">
