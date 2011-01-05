def relpath(path, start = '.'):
    from os.path import abspath, commonprefix, sep, pardir, join

    origin = abspath(start).split(sep)
    dest   = abspath(path).split(sep)
    
    pos = len(commonprefix([origin, dest]))
    components = [pardir] * (len(origin) - pos) + dest[pos:]
    
    if not components:
        return '.'

    return join(*components)
