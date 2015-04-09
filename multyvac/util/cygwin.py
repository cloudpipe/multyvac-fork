import os

class WindowsPath(object):
    """
    Processes Windows paths for easy conversion into cwRsync compatible
    format.  This involves replacing drive specification into
    /cygdrive/[drive_lettter] format, and replacing backward slashes with
    forward slashes.  While backward slashes will allow correctly locating the
    filesystem resource locally, it most likely will result in incorrect
    destinations on the remote side, as backward slashes will be interpreted as
    part of the name itself.
    """
    def __init__(self, path):
        """Path is any (absolute or relative) Windows path."""
        if os.path.isabs(path):
            drive, tail = os.path.splitdrive(path)
            self.drive = drive.rstrip(':')
            self.tail = tail.lstrip('\\').lstrip('/')
        else:
            self.drive = ''
            self.tail = path.lstrip('\\').lstrip('/')

    def to_cygpath(self):
        """
        This is where the rsync safe conversion takes place.  If there is a
        space in the path, encapsulates the path with quotes to assure the path
        will not be misinterpreted as being two or more paths.
        """
        drive = os.path.join('\\cygdrive', self.drive) if self.drive else ''
        path = os.path.join(drive, self.tail)
        path = path.replace('\\', '/')
        if " " in path:
            path = '"%s"' % path
        return path

def regularize_path(path):
    if os.name == 'nt':
        return WindowsPath(path).to_cygpath()
    else:
        return path
