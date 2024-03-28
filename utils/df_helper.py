def mergedicts(dict1, dict2):
  for k in set(dict1.keys()).union(dict2.keys()):
    if k in dict1 and k in dict2:
      if isinstance(dict1[k], dict) and isinstance(dict2[k], dict):
        yield (k, dict(mergedicts(dict1[k], dict2[k])))
      else:
      # If one of the values is not a dict, you can't continue merging it.
      # Value from second dict overrides one in first and we move on.
        yield (k, dict2[k])
      # Alternatively, replace this with exception raiser to alert you of value conflicts
    elif k in dict1:
      yield (k, dict1[k])
    else:
      yield (k, dict2[k])

def parse_nested_grouped_df_to_dict_v2(df):
    """
    >>> keys = 1, 2, 3
    >>> value = 5
    result --> {1: {2: {3: 5}}}
    """

    master_list = [] # store all the records in dictionary format in this list

    # this for loop is iterating over each record and converting index to nested dictionary
    for idx in df.index:
        # all the indexes in tuple to be converted to list
        keys = list(idx)
        # get the record value based on the indexes
        value = df.loc[idx, :].to_dict()

        it = iter(keys)
        last = next(it)
        res = {last: {}}
        lvl = res

        # this logic to create multiple levels of nested dictionary
        while True:
            try:
                k = next(it)
                lvl = lvl[last]
                lvl[k] = {}
                last = k
            except StopIteration:
                lvl[k] = value
                master_list.append(res)
                break

    resp_df = {}

    for d in master_list:
        # call the mergedicts function 
        resp_df = dict(mergedicts(resp_df, d))

    return resp_df

