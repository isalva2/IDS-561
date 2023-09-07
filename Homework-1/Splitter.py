def splitter(keyval, num_splits):
    
    # calculate the number of keyval pairs that will be in each split
    split_size = len(keyval)//num_splits
    
    # empty return object
    splits = []

    # 
    for i in range(num_splits):
        start = i*split_size