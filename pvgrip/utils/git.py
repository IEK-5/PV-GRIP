import subprocess


def git_root():
    """Get a root directory of a git repository

    :return: string, a local path
    """
    res = subprocess.run(["git","rev-parse","--show-toplevel"],
                         stdout=subprocess.PIPE).\
                         stdout.decode().split('\n')[0]
    return res
