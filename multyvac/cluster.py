import json

from .multyvac import (
    Multyvac,
    MultyvacModel,
    MultyvacModule,
)

class Cluster(MultyvacModel):
    """Represents a Multyvac Cluster and its associated operations."""
    
    def __init__(self, id, **kwargs):
        """Creates a new Cluster."""
        MultyvacModel.__init__(self, **kwargs)
        
        self.id = id
        self.state = kwargs.get('state')
        self.requested_at = kwargs.get('requested_at')
        self.provisioned_at = kwargs.get('provisioned_at')
        self.released_at = kwargs.get('released_at')
        
        self.core = kwargs.get('core')
        self.core_count = kwargs.get('core_count')
        self.duration = kwargs.get('duration')
        self.max_duration = kwargs.get('max_duration')

    def release(self):
        """Releases the cluster's resources."""
        r = self.multyvac._ask(Multyvac._ASK_POST,
                               '/cluster/%s/release' % self.id)
        return MultyvacModule.check_success(r)

    def update_max_duration(self, max_duration):
        """Releases the cluster's resources."""
        r = self.multyvac._ask(Multyvac._ASK_PATCH,
                               '/cluster/%s/update_max_duration' % self.id,
                               data={'max_duration': max_duration},
                               )
        return MultyvacModule.check_success(r)
    
    def update(self):
        """Updates this Cluster object with the latest version available of
        itself from Multyvac."""
        c = self.multyvac.cluster.get(self.id)
        self.__dict__ = c.__dict__
        return self.state

    def __repr__(self):
        return 'Cluster(%s)' % repr(self.id)

class ClusterModule(MultyvacModule):
    """Top-level Cluster module. Use this through ``multyvac.cluster``."""
    
    def get(self, id):
        """
        Returns a Cluster object.
        
        :param id: Id of cluster.
        """
        
        r = self.multyvac._ask(Multyvac._ASK_GET,
                               '/cluster/%s' % id)
        c = r['cluster']
        if c['requested_at']:
            c['requested_at'] = MultyvacModule.convert_str_to_datetime(c['requested_at'])
        if c['provisioned_at']:
            c['provisioned_at'] = MultyvacModule.convert_str_to_datetime(c['provisioned_at'])
        if c['released_at']:
            c['released_at'] = MultyvacModule.convert_str_to_datetime(c['released_at'])
        return Cluster(multyvac=self.multyvac, **r['cluster'])
    
    def list(self):
        """
        Lists all active clusters, and recently provisioned and released ones.
        """
        
        r = self.multyvac._ask(Multyvac._ASK_GET,
                               '/cluster')
        for c in r['clusters']:
            if c['requested_at']:
                c['requested_at'] = MultyvacModule.convert_str_to_datetime(c['requested_at'])
            if c['provisioned_at']:
                c['provisioned_at'] = MultyvacModule.convert_str_to_datetime(c['provisioned_at'])
            if c['released_at']:
                c['released_at'] = MultyvacModule.convert_str_to_datetime(c['released_at'])
        
        return [Cluster(multyvac=self.multyvac, **cluster) for cluster in r['clusters']]
    
    def provision(self, core, core_count, max_duration=None):
        """
        Provisions a new cluster.
        
        :param core: The core type to provision.
        :param core_count: The number of cores to provision.
        :param duration: The number of hours to keep cluster up for.
        """
        
        cluster = {'core': core,
                   'core_count': core_count,
                   'max_duration': max_duration,
                   }
        MultyvacModule.clear_null_entries(cluster)
        payload = {'cluster': cluster}
        headers = {'content-type': 'application/json'}
        r = self.multyvac._ask(Multyvac._ASK_POST,
                               '/cluster',
                               data=json.dumps(payload),
                               headers=headers)
        return r['id']
    
