def Mapper(tokens):
    
    # return object
    keyvals = []

    # append key-value pairs for eack item in tokens
    for token in tokens:
        keyvals.append([token, 1])

    return keyvals
