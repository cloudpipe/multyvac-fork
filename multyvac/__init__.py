import os as _os
from pkg_resources import (
    get_distribution as _get_distribution,
    DistributionNotFound as _DistributionNotFound,
)

from .multyvac import (
    Multyvac,
    MultyvacError,
    RequestError,
)

# Automatically determine the version of the package
try:
    _dist = _get_distribution('multyvac')
    if not __file__.startswith(_os.path.join(_dist.location, 'multyvac')):
        # not installed, but there is another version that *is*
        raise _DistributionNotFound
except _DistributionNotFound:
    __version__ = 'Only available when installed with setup.py'
else:
    # FIXME: If there are two egg-info folders (an older version failed to
    # uninstall), then the older one is chosen, which is incorrect.
    __version__ = _dist.version

# Default Multyvac singleton
_multyvac = Multyvac()

on_multyvac = _multyvac.on_multyvac
send_log_to_support = _multyvac.send_log_to_support

# Job methods that have been elevated to top level
from .job import JobError
modulemgr = _multyvac.job._modulemgr
get = _multyvac.job.get
get_by_name = _multyvac.job.get_by_name
list = _multyvac.job.list
kill = _multyvac.job.kill
kill_all = _multyvac.job.kill_all
wait = _multyvac.job.wait
shell_submit = _multyvac.job.shell_submit
submit = _multyvac.job.submit
queue_stats = _multyvac.job.queue_stats

# All other modules
from .config import ConfigError
config = _multyvac.config
from .volume import SyncError
volume = _multyvac.volume
layer = _multyvac.layer
cluster = _multyvac.cluster
api_key = _multyvac.api_key
