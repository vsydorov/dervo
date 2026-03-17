import logging
import math

log = logging.getLogger(__name__)


def indent_mstring(string, indent=4):
    """Indent multiline string"""
    return '\n'.join(map(lambda x: ' '*indent+x, string.split('\n')))


def enumerate_mstring(string, indent=4):
    estring = []
    splitted = string.split('\n')
    maxlen = math.floor(math.log(len(splitted), 10))+1
    for ind, line in enumerate(splitted):
        estring.append('{0:{1}d}{2}{3}'.format(
            ind+1, maxlen, ' '*indent, line))
    return '\n'.join(estring)


def force_symlink(path, linkname, where):
    """
    Force symlink creation. If symlink to wrong place - fail

    Important to be careful when resolving relative paths
    """
    link_fullpath = path/linkname
    where_fullpath = path/where
    if link_fullpath.is_symlink():
        r_link = link_fullpath.resolve()
        r_where = where_fullpath.resolve()
        assert r_link == r_where, \
                ('Symlink exists, but points to a wrong '
                'place {} instead of {}').format(r_link, r_where)
    else:
        for i in range(256):
            try:
                link_fullpath.symlink_to(where)
                break
            except (FileExistsError, FileNotFoundError) as e:
                log.debug('Try {}: Caught {}, trying again'.format(i, e))
            finally:
                log.debug('Managed at try {}'.format(i))
