import configparser

def get_configs(fn):
    config = configparser.ConfigParser()
    config.read(fn)
    return config
