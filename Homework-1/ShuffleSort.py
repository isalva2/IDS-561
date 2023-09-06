import itertools

def ShuffleSort(*keyvals):

    # combine output of multiple Mappers
    combined_keyvals = list(itertools.chain(*keyvals))
    
    # sort on keys
    combined_keyvals = sorted(combined_keyvals, key = lambda x: x[0])

    return keyvals