import os
import re
import logging
import tempfile


from pvgrip.utils.git \
    import git_root


def get_tempfile(path = os.path.join(git_root(),
                                     'data','tempfiles')):
    os.makedirs(path,exist_ok = True)
    fd = tempfile.NamedTemporaryFile(dir = path, delete = False)
    return os.path.join(path,fd.name)


def get_tempdir(path = os.path.join(git_root(),
                                    'data','tempfiles')):
    os.makedirs(path,exist_ok = True)
    return tempfile.mkdtemp(dir = path)


def remove_file(fn):
    try:
        if fn:
            os.remove(fn)
    except:
        logging.error("cannot remove file: %s" % fn)
        pass


def move_file(src, dst, link = False):
    os.makedirs(os.path.dirname(dst), exist_ok = True)

    if link:
        if os.path.exists(dst):
            os.remove(dst)
        os.link(src, dst)
        return

    os.replace(src, dst)


def list_files(path, regex):
    r = re.compile(regex)
    return [os.path.join(dp, f) \
            for dp, dn, filenames in \
            os.walk(path) \
            for f in filenames \
            if r.match(os.path.join(dp, f))]
