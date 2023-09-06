def Reducer(keyvals):

    # empty return dict
    keyvalues = {}

    for key, value in keyvals:
        if key not in keyvalues.keys():
            keyvalues[key] = value
        else:
            keyvalues[key] += value

    return keyvalues