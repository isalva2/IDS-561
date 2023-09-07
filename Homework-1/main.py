import re
import ast
import itertools

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

def splitter(keyval_pairs, num_splits = 2):

    '''
    This function splits the dataset into an arbitrary number of splits, 2 at the minimum.
    '''

    # get indices for splits
    split_size = len(keyval_pairs) // num_splits

    # empty return list
    splits = []

    # index splits and add to return list
    for i in range (num_splits):
        start =  i*split_size
        end = (i+1)*split_size if i < num_splits - 1 else None
        split = keyval_pairs[start:end]
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

