import base64
import copy
try:
    import cPickle as pickle
except:
    import pickle
try:
    from cStringIO import StringIO
except:
    import StringIO
from functools import partial
import inspect
import numbers
import socket
import subprocess
import time

from .multyvac import (
    Multyvac,
    MultyvacError,
    MultyvacModel,
    MultyvacModule,
    RequestError,
)
from .util import preinstalls
from .util.cygwin import regularize_path
from .util.module_dependency import ModuleDependencyAnalyzer

class JobError(MultyvacError):
    """Exception class for errors encountered by a job."""
    pass

class Job(MultyvacModel):
    """Represents a Multyvac Job and its associated operations."""
    
    status_waiting = 'waiting'
    status_queued = 'queued'
    status_processing = 'processing'
    
    status_done = 'done'
    status_error = 'error'
    status_killed = 'killed'
    status_stalled = 'stalled'
    
    finished_statuses = [status_done,
                         status_error,
                         status_killed,
                         status_stalled]
    
    def __init__(self, jid, **kwargs):
        MultyvacModel.__init__(self, **kwargs)
        self.jid = jid
        
        self.cmd = kwargs.get('cmd')
        self.core = kwargs.get('core')
        self.created_at = kwargs.get('created_at')
        self.multicore = kwargs.get('multicore')
        self.name = kwargs.get('tags', {}).get('name')
        self.status = kwargs.get('status')
        self.tags = kwargs.get('tags')
    
        result = kwargs.get('result')
        self.result_type = kwargs.get('result_type')
        if result and self.result_type  == 'pickle':
            self.result = pickle.loads(base64.b64decode(result))
        elif result and self.result_type == 'binary':
            self.result = base64.b64decode(result)
        else:
            self.result = result
        self.return_code = kwargs.get('return_code')
        
        self.started_at = kwargs.get('started_at')
        self.finished_at = kwargs.get('finished_at')
        self.runtime = kwargs.get('runtime')
        self.max_runtime = kwargs.get('max_runtime')
        self.queue_delay = kwargs.get('queue_delay')
        self.overhead_delay = kwargs.get('overhead_delay')
        
        self.cputime_user = kwargs.get('collected', {}).get('cputime_user')
        self.cputime_system = kwargs.get('collected', {}).get('cputime_system')
        self.memory_failcnt = kwargs.get('collected', {}).get('memory_failcnt')
        self.memory_max_usage = kwargs.get('collected', {}).get('memory_max_usage')
        self.ports = kwargs.get('collected', {}).get('ports')
        self.stderr = kwargs.get('stderr')
        self.stdout = kwargs.get('stdout')
    
    def get_result(self, raise_on_error=True):
        """
        Better than using the result attribute directly.
        
        First, it waits for the job to finish processing. Once finished,
        returns the result. If the job encountered an error (or stalled/killed)
        a JobError exception is raised.
        
        :param raise_on_error: If set to False, an exception is returned,
            rather than raised, in the event of an error.
        """
        self.wait()
        if self.status != self.status_done:
            e = JobError(self.stderr)
            if raise_on_error:
                raise e
            else:
                return e
        else:
            return self.result
    
    def kill(self):
        """
        Kills this job.
        """
        data = {'jid': self.jid}
        r = self.multyvac._ask(Multyvac._ASK_POST,
                               '/job/kill',
                               data=data)
        return MultyvacModule.check_success(r)
    
    def wait(self, status=finished_statuses, timeout=None):
        """
        Wait for the job to reach the specified status.
        
        :param status: Can be a status string or a list of strings.
        
        Returns the status if the job reaches it, otherwise on a timeout
        returns False.
        """
        start_time = time.time()
        poll_period = 1
        max_poll_period = 10
        while True:
            current_time = time.time()
            time_elapsed = current_time - start_time
            if timeout and time_elapsed > timeout:
                return False
            elif self.status == status or self.status in status:
                return self.status
            else:
                sleep_time = (min(poll_period, timeout - time_elapsed)
                              if timeout else poll_period)
                time.sleep(sleep_time)
                if poll_period < max_poll_period:
                    poll_period += 0.5
                self.update()
    
    def wait_for_open_port(self, port, timeout=None):
        """
        Blocks until it detects that port has been opened by the job.
        Returns a dict of the external address and port combination (will
        be different than the input port due to NAT).
        
        If port is 22, also returns path to identity file, and username.
        
        :param timeout: The amount of time to wait for the job to start.
        
        Once started, the job must open the port within 10 seconds. Otherwise,
        False is returned.
        """
        port = str(port)
        self.wait(self.finished_statuses + [self.status_processing], timeout)
        if self.status == self.status_processing:
            attempt = 1
            max_attempts = 10
            while attempt < max_attempts and self.status == Job.status_processing:
                if not self.ports or not self.ports.get('tcp', {}).get(port):
                    # Wait for SSH to start
                    time.sleep(2.0)
                    attempt += 1
                    self.update()
                else:
                    d = copy.copy(self.ports.get('tcp', {}).get(port))
                    if port == '22':
                        d['username'] = 'multyvac'
                        d['identity_file'] = self.multyvac.config.path_to_private_key()
                    return d
            else:
                return False
        else:
            return False
    
    def open_ssh_console(self):
        """Opens an SSH console to a running job. If a job is queued, waits
        indefinitely until it is processing."""
        
        if self.wait_for_open_port(22):
            info = self.ports.get('tcp', {}).get('22')
            address = info['address']
            port = info['port']
            cmd = ('{ssh_bin} -o UserKnownHostsFile=/dev/null '
                   '-o StrictHostKeyChecking=no -X -p {port} -i {key_path} '
                   ' multyvac@{address}'.format(
                        ssh_bin=self.multyvac._ssh_bin,
                        port=port,
                        key_path=regularize_path(self.multyvac.config.path_to_private_key()),
                        address=address,)
                   )
            p = subprocess.Popen(cmd, shell=True)
            p.wait()
        
    def run_command(self, cmd):
        """
        Runs the specified command over ssh. Blocks until the job has begun
        processing.
        
        If successful, returns (stdout, stderr). Otherwise, returns False,
        which happens most notably if the job has already finished by the
        time this command is run.
        """
        
        if self.wait_for_open_port(22):
            info = self.ports.get('tcp', {}).get('22')
            address = info['address']
            port = info['port']
            cmd = ('{ssh_bin} -o UserKnownHostsFile=/dev/null '
                   '-o StrictHostKeyChecking=no -p {port} -i {key_path} '
                   ' multyvac@{address} {cmd}'.format(
                        ssh_bin=self.multyvac._ssh_bin,
                        port=port,
                        key_path=regularize_path(self.multyvac.config.path_to_private_key()),
                        address=address,
                        cmd=cmd)
                   )
            p = subprocess.Popen(cmd,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True)
            return p.communicate()
        else:
            self.multyvac.job._logger.info('Cannot SSH into finished job')
            return False
    
    def update(self):
        """Updates this Job object with the latest version available of itself
        from Multyvac."""
        j = self.multyvac.job.get(self.jid)
        self.__dict__ = j.__dict__
        return self.status

    def __repr__(self):
        if self.name:
            return '%s(%s, name=%s)' % (self.__class__.__name__,
                                        repr(self.jid), repr(self.name))
        else:
            return '%s(%s)' % (self.__class__.__name__, repr(self.jid))


class JobModule(MultyvacModule):
    """Most of JobModule's methods are exposed directly through ``multyvac``.
    For example, ``multyvac.submit()``."""

    def __init__(self, *args, **kwargs):
        MultyvacModule.__init__(self, *args, **kwargs)
        self._modulemgr = ModuleDependencyAnalyzer()
        preinstalled_modules = [name for name, _ in preinstalls.modules]
        self._modulemgr.ignore(preinstalled_modules)
    
    def _normalize_vol(self, vol):
        if isinstance(vol, basestring):
            return [{'name': vol}]
        elif isinstance(vol, dict):
            return [vol]
        elif hasattr(vol, '__iter__'):
            if isinstance(vol[0], dict):
                return vol
            elif isinstance(vol[0], basestring):
                return [{'name': v} for v in vol]
            else:
                raise TypeError('vol must be a iterable of strings or dicts')
        else:
            raise TypeError('vol must be a string, dict, or iterable')
        
    def _normalize_layer(self, layer):
        if isinstance(layer, basestring):
            return [{'name':layer}]
        elif isinstance(layer, dict):
            return [layer]
        else:
            raise TypeError('layer must be a string or dict')

    def shell_submit(self, cmd, _name=None, _core='c1', _multicore=1,
                     _layer=None,  _vol=None, _env=None,
                     _result_source='stdout', _result_type='binary',
                     _max_runtime=None, _profile=False, _restartable=True,
                     _tags=None, _depends_on=None, _stdin=None):
        """
        Submit a job to Multyvac.
        
        :param cmd: The shell command to execute.
        :param _name: The string name to give the job.
        :param _core: The core to use.
        :param _multicore: The number of cores to assign to the job.
        :param _layer: The name of the layer to use.
        :param _vol: The name or a list of names of volumes to mount.
        :param _env: A dictionary of any environment variables that should be
            set in the shell running the job.
        :param _result_source: The source of the result. By default it's the
            standard output of the job. But it can also be set to 'file:{path}'
            where {path} should be replaced by the location of a file that will
            contain the job's result.
        :param _result_type: The format of the job's result. By default, it is
            set to binary, which means Multyvac cannot interpret its contents.
        :param _max_runtime: The maximum number of minutes this job should be
            allowed to run before it is forcibly killed.
        :param _profile: Not implemented yet.
        :param _restartable: If a server running the job fails unexpectedly,
            can this job be safely restarted?
        :param _tags: A dict mapping keys to values of arbitrary data. Good for
            storing job metadata.
        :param _depends_on: Not implemented.
        :param _stdin: The standard input that should be piped into the job.
        
        :returns: Job id.
        """
        
        job = {
               'cmd': cmd,
               'name': _name,
               'core': _core,
               'multicore': _multicore,
               'profile': _profile,
               'restartable': _restartable,
               'depends_on': _depends_on,
               'tags': _tags,
               'layer': self._normalize_layer(_layer) if _layer else None,
               'vol': self._normalize_vol(_vol) if _vol else None,
               'env': _env,
               'result_source': _result_source,
               'result_type': _result_type,
               'max_runtime': _max_runtime,
               }
        
        if _stdin:
            job['stdin'] = base64.b64encode(_stdin)
        
        MultyvacModule.clear_null_entries(job)
        
        payload = {'jobs': [job]}
        
        r = self.multyvac._ask(Multyvac._ASK_POST,
                               '/job',
                               data=payload,
                               content_type_json=True)
        return r['jids'][0]

    def _get_auto_module_volume_name(self):
        return 'auto-deps-%s' % socket.gethostname()

    def submit(self, f, *args, **kwargs):
        """
        Submit a Python function as a job to Multyvac.
        
        :param args: The positional arguments to pass into f.
        :param kwargs: The keywords arguments to pass info f. kwargs may also
            include special keys that are prefixed with '_'. See the arguments
            to :meth:`shell_submit` for more information.
            
        Set _ignore_module_dependencies=True as a keyword to prevent module
        dependencies from being automatically sync-ed. Do this only if you have
        setup a layer with all of your dependencies pre-installed.
            
        :returns: Job id.
        """
        
        f_kwargs = {}
        for k, v in kwargs.items():
            if not k.startswith('_'):
                f_kwargs[k] = v
                del kwargs[k]
        
        from .util.cloudpickle import CloudPickler
        
        s = StringIO()
        cp = CloudPickler(s, 2)
        cp.dump((f, args, f_kwargs))
        
        if '_ignore_module_dependencies' in kwargs:
            ignore_modulemgr = kwargs['_ignore_module_dependencies']
            del kwargs['_ignore_module_dependencies']
        else:
            ignore_modulemgr = False
            
        if not ignore_modulemgr:
            # Add modules
            for module in cp.modules:
                self._modulemgr.add(module.__name__)
            
            mod_paths = self._modulemgr.get_and_clear_paths()
            
            vol_name = self._get_auto_module_volume_name()
            if self._modulemgr.has_module_dependencies:
                v = self.multyvac.volume.get(vol_name)
                if not v:
                    try:
                        self.multyvac.volume.create(vol_name, '/pymodules')
                    except RequestError as e:
                        if 'name already exists' not in e.message:
                            raise
                    v = self.multyvac.volume.get(vol_name)
                if mod_paths:
                    v.sync_up(mod_paths, '')
            
        kwargs['_stdin'] = s.getvalue()
        kwargs['_result_source'] = 'file:/tmp/.result'
        kwargs['_result_type'] = 'pickle'
        if not ignore_modulemgr and self._modulemgr.has_module_dependencies:
            kwargs.setdefault('_vol', []).append(vol_name)
        # Add to the PYTHONPATH if user is using it as well
        env = kwargs.setdefault('_env', {})
        if env.get('PYTHONPATH'):
            env['PYTHONPATH'] = env['PYTHONPATH'] + ':/pymodules'
        else:
            env['PYTHONPATH'] = '/pymodules'
            
        tags = kwargs.setdefault('_tags', {})
        # Make sure function name fits within length limit for tags
        fname = JobModule._func_name(f)
        if len(fname) > 100:
            fname = fname[:97] + '...'
        tags['fname'] = fname
        
        return self.shell_submit(
            'python -m multyvacinit.pybootstrap',
            **kwargs
        )

    def list(self,
             jid=None,
             name=None,
             limit=50,
             before=None,
             after=None,
             status=None):
        """
        Query jobs.
        
        :param jid: Can be a job id or list of job ids.
        :param name: Can be a name or list of names.
        :param limit: Maximum number of jobs to return.
        :param before: Only jobs that were created before this jid are returned.
        :param after: Only jobs that were created after this jid are returned.
        :param status: Only jobs with this status are returned.
            Can be a list of statuses.
        
        Use `before` and `after` to paginate through jobs efficiently. 
            
        :returns: A list of matching jobs.
        """
        params = {'jid': jid,
                  'name': name,
                  'limit': limit,
                  'before': before,
                  'after': after,
                  'status': status,
                  #'field': self.fields,
                  }
        MultyvacModule.clear_null_entries(params)
        
        return self._get(params)

    def get(self, jid, fields=None):
        """
        Returns a Job with corresponding id.
        
        :param jid: Can be a job id or list of job ids.
        
        :returns: A list if input was a list, otherwise returns an object.
            Also, can return None if a single jid is requested, and it cannot
            be found.
        """
        iter_in = MultyvacModule.is_iterable_list(jid)
        
        if iter_in:
            jid_to_jobs = {}
            for jids_chunk in MultyvacModule.list_chunker(jid, 50):
                jobs = self._get({'jid': jids_chunk}, fields)
                for job in jobs:
                    jid_to_jobs[job.jid] = job
            jobs = []
            for i in jid:
                if i not in jid_to_jobs:
                    raise ValueError('Could not find job %s' % i)
                jobs.append(jid_to_jobs[i])
            return jobs
        else:
            params = {'jid': jid,
                      'limit': 1}
            jobs = self._get(params, fields)
            return jobs[0] if jobs else None
    
    def get_by_name(self, name, fields=None):
        """
        Get a job by name.
        
        :param name: The name of the job.
        
        :returns: The most recent job with the matching name. Use :meth:`list`
            to get a list of all matching jobs.
        """
        
        iter_in = MultyvacModule.is_iterable_list(name)
        if iter_in:
            raise ValueError('name can only be a string, not a list')
        
        params = {'name': name,
                  'limit': 1}
        jobs = self._get(params, fields)
        
        return jobs[0] if jobs else None
    
    def _get(self, more_params, fields=None):
        
        params = {}
        if fields:
            params['field'] = fields
        params.update(more_params)
        
        r = self.multyvac._ask(Multyvac._ASK_GET,
                               '/job',
                               params=params)
        for job in r['jobs']:
            if 'created_at' in job:
                job['created_at'] = MultyvacModule.convert_str_to_datetime(job['created_at'])
        return [Job(multyvac=self.multyvac, **job) for job in r['jobs']]

    @staticmethod
    def _func_name(func):
        """Return name of a callable (function, class, partial, etc.)"""
        module = ''
        if hasattr(func,'__module__'):
            module = (func.__module__ if func.__module__ else '__main__')
        # Return a human readable name associated with a function
        if inspect.ismethod(func):
            nme = '.'.join([module,func.im_class.__name__,func.__name__])
        elif inspect.isfunction(func):
            nme =  '.'.join([module,func.__name__])
        elif inspect.isbuiltin(func):
            return  '.'.join([module,func.__name__])
        elif isinstance(func,partial):
            return 'partial_of_' + JobModule._func_name(func.func)
        elif inspect.isclass(func):
            nme = '.'.join([module,func.__name__])
            if hasattr(func, '__init__') and inspect.ismethod(func.__init__):            
                func = func.__init__
            else:
                return nme
        else:
            nme = 'type %s' % type(func)
            if hasattr(func, '__name__'):
                nme = '%s of %s' % (func.__name__, type(func))
            return nme
        nme +=  ' at ' + ':'.join([func.func_code.co_filename,
                                   str(func.func_code.co_firstlineno)])
        return nme

    def kill(self, jid):
        """
        Kills a job. If the job is queued, it will never run. If it's
        processing, it will be abruptly stopped. If it's already finished,
        nothing changes.
        
        :param jid: A job id or list of job ids to kill.
        """
        
        data = {'jid': jid}
        r = self.multyvac._ask(Multyvac._ASK_POST,
                               '/job/kill',
                               data=data)
        return MultyvacModule.check_success(r)

    def kill_all(self):
        """
        Kills all unfinished jobs.
        """
        r = self.multyvac._ask(Multyvac._ASK_POST,
                               '/job/kill_all')
        return MultyvacModule.check_success(r)
    
    def wait(self, jobs_or_jids, timeout=None):
        """
        An efficient way to get the results for a batch of jobs.
        
        Blocks until all jobs are finished, and then returns a list of Jobs
        with output information (result, stdout, stderr, ...) already
        retrieved. Call each returned job's respective `get_result()` function.
        
        :param jobs_or_jids: A list of Job objects or jids.
        :param float timeout: If the jobs have not finished by this many
            seconds, the functions return None.
        
        :returns: A list of jobs.
        """
        
        if not hasattr(jobs_or_jids, '__iter__'):
            raise ValueError('jobs_or_jids must be iterable')
        
        jids = []
        for j in jobs_or_jids:
            if isinstance(j, Job):
                jids.append(j.jid)
            elif isinstance(j, numbers.Integral):
                jids.append(j)
            else:
                raise ValueError('Elements in jobs_or_jids cannot be of '
                                 'type %s' % type(j))
        
        tries = 1
        start_time = time.time()
        unfinished_jids = jids[:]
        while unfinished_jids:
            current_time = time.time()
            time_elapsed = current_time - start_time
            if timeout and time_elapsed > timeout:
                return None
            jobs = self.get(unfinished_jids, fields=['jid', 'status'])
            for job in jobs:
                if job.status in Job.finished_statuses:
                    unfinished_jids.remove(job.jid)
            if unfinished_jids:
                tries += 1
                time.sleep(1.0 + min(tries/10.0, 9.0))
        
        return self.get(jids)
    
    def queue_stats(self):
        """
        Returns a dict that shows the number of jobs that are queued and
        processing.
        """
        
        r = self.multyvac._ask(Multyvac._ASK_GET,
                               '/job/queue_stats')
        return r['stats']

    def invoice(self, d):
        """
        Returns a dict that shows the number of jobs that are queued and
        processing.
        """
        
        r = self.multyvac._ask(Multyvac._ASK_GET,
                               '/invoice/%s' % d)
        return r

