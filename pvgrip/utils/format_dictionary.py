def format_dictionary(d):
    """Print dictionary

    :d: dictionary

    :return: string
    """
    res = ""
    for key, item in d.items():
        res += "{} = {}\n".format(key,item)
    return res
