import re
import ast

# preprocessing function
def preprocessing(file):

    '''
    This function returns a list of strings in the format "(YEAR, MONTH), (YEAR MONTH)"
    '''
    
    # empty return object
    output = []

    # read file contents
    with open(file, "r") as content:
        for line in content:

            # append to list and remove newline character "\n"
            output.append(line[:-1])

    '''
    # use regular expression to extract tuples
    tuples = re.findall(r'\((\d+),\s*(\d+)\)', file_contents)

    # convert to int for downstream processing, remove month from keys
    keyval_pairs = [(int(value1[:-2]), int(value2)) for value1, value2 in tuples]
    '''

    return output

# splitter function
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
    
# mapper
def mapper(split):

    # create empty return list
    keyval_pairs = []

    # loop through input and append 
    for string in split:

        # use ast method to return tuples of integers
        literal_eval = ast.literal_eval(string)

        # append tuple pairs to return list
        keyval_pairs.append(literal_eval[0])
        keyval_pairs.append(literal_eval[1])
    
    # change key to approriate year format
    keyval_pairs = [(int(str(keyval[0])[:-2]), keyval[1]) for keyval in keyval_pairs]

    return keyval_pairs

# ShuffleSort