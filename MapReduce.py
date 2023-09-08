import re
import ast
import itertools
import pandas as pd
from time import time

def preprocessing(file):

    '''
    This function returns a list of strings in the format "(YEAR-MONTH, TEMP), (YEAR-MONTH, TEMP)"
    '''
    
    # empty return list
    output = []

    # read file contents
    with open(file, "r") as content:
        for line in content:

            # append to list and remove newline character "\n"
            output.append(line[:-1])

    return output

def splitter(input, num_splits = 2):

    '''
    This function splits the dataset into an arbitrary number of n splits (n = 2 by default).
    '''

    # get indices for splits
    split_size = len(input) // num_splits

    # empty return list
    splits = []

    # index splits and add to return list
    for i in range (num_splits):
        start =  i*split_size
        end = (i+1)*split_size if i < num_splits - 1 else None
        split = input[start:end]
        splits.append(split)

    return splits
    
def mapper(split):

    '''
    This maps the input split into key-value pairs and modifies the key to year only. Returns a list of tuples in the form of (YEAR, TEMP)
    '''

    # create empty return list
    keyval_pairs = []

    # loop through input and append 
    for string in split:

        # use ast method to return tuples of integers
        literal_eval = ast.literal_eval(string)

        # append tuple pairs to return list
        keyval_pairs.append(literal_eval[0])
        keyval_pairs.append(literal_eval[1])
    
    # change key to appropriate year format
    keyval_pairs = [(int(str(keyval[0])[:-2]), keyval[1]) for keyval in keyval_pairs]

    return keyval_pairs

def sort(*multiple_keyval_pairs):

    '''
    This function combines the seperated key-value pairs and sorts on the year key
    '''
    
    # recombine keyval_pairs
    recombined_keyval_pairs = list(itertools.chain(*multiple_keyval_pairs))

    # sort values
    sorted_keyval_pairs = sorted(recombined_keyval_pairs, key = lambda x: x[0])

    return sorted_keyval_pairs

def partition(sorted_keyval_pairs, partitions = 2):
    
    '''
    This function takes the sorted key-value pairs and seperates them into n partitions (n = 2 by default).
    '''

    # obtain unique keys from the sorted key-value pairs
    unique_years = {year for year, _ in sorted_keyval_pairs}
    unique_years = sorted(list(unique_years))

    # index unique years for partitioning
    partition_size = len(unique_years) // partitions

    year_splits = []
    
    for i in range (partitions):
        start =  i*partition_size
        end = (i+1)*partition_size if i < partitions - 1 else None
        split = unique_years[start:end]
        year_splits.append(split)

    # return partitions
    partitions = []

    for years in year_splits:
        partition = [keyval_pair for keyval_pair in sorted_keyval_pairs if keyval_pair[0] in years]
        partitions.append(partition)

    return partitions

def reducer(partition):

    '''
    This function performs the search for maximum temperature for each year.
    '''

    # empty return dict
    reduced_keyval_pairs = {}

    for key, value in partition:
        if key not in reduced_keyval_pairs.keys():
            reduced_keyval_pairs[key] = value
        else:
            if reduced_keyval_pairs[key] < value:
                reduced_keyval_pairs[key] = value
    
    return reduced_keyval_pairs

def main():

    '''
    This main function is the final driver for the MapReduce task.
    '''

    # measure MapReduce run time
    start = time()

    # get input file
    file = "data/temperatures.txt"

    # preprocess data
    clean_text = preprocessing(file)

    # split data
    split1, split2 = splitter(clean_text)

    # pass splits to two mappers
    mapper1 = mapper(split1)
    mapper2 = mapper(split2)

    # shuffle sort mapper outputs
    sorted_keyval_pairs = sort(mapper1, mapper2)

    # partition sorted key-value pairs
    partition1, partition2 = partition(sorted_keyval_pairs)

    # pass partition to two reducers
    reducer1 = reducer(partition1)
    reducer2 = reducer(partition2)

    # combine reducers
    combined_reducers = {**reducer1, **reducer2}

    # end run time
    end = time()

    # convert to df
    df = pd.DataFrame.from_dict(combined_reducers, orient='index', columns=['Value']).reset_index()
    df.columns = ['year', 'Max temp']

    # print and save
    print("Total running time = {} s\n".format(end-start))
    print(df)

    df.to_csv("data/output.csv", index = False)

if __name__ == '__main__':
    main()