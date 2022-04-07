"""

1.  Function to create a single hash per directory of files
    + key pair value for all files within dir & its respective hash values

         dirhash('/path/to/directory')
         Output:
               {'singledirhash': {'/tmp/cbs_backup_1533529247/': '650aefa1acd9b438ab3cfdaa10925c2e'},
                'allfileshash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': '9658fa063322ce28ba65ef0bbf6fb4c3'}
               }

2.  Function to create hash for a file

      _filehash('/path/to/file')
         Output:
               {'singledirhash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': 'Its a file'},
                'allfileshash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': '9658fa063322ce28ba65ef0bbf6fb4c3'}
               }

"""

import os
import hashlib
import re

HASH_FUNCS = {
    'md5': hashlib.md5,
    'sha1': hashlib.sha1,
    'sha256': hashlib.sha256,
    'sha512': hashlib.sha512
}


class md5sum(object):
    def __init__(self, device=None):
        self.device = device
        if self.device:
            self.sftp = self.device.ssh.open_sftp()
            tmp_dir = '/tmp/md5.py'
            self.sftp.put(__file__, tmp_dir)
        self.hash = 'md5'
        self.hash_func = HASH_FUNCS.get(self.hash)
        self.hasher = self.hash_func()

    def dirhash(self, path, followlinks=False):
        '''
        Method to create md5 hash for any directory or file

        :param ``path``: directory or file path to perform hash

        :sample 
            dirhash('/tmp/cbs_backup_1533529247/')
            dirhash('/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000')

        :return dict
            {'singledirhash': {'/tmp/cbs_backup_1533529247/': '650aefa1acd9b438ab3cfdaa10925c2e'},
                'allfileshash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': '9658fa063322ce28ba65ef0bbf6fb4c3'}
            }

            OR

            {'singledirhash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': 'Its a file'},
                'allfileshash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': '9658fa063322ce28ba65ef0bbf6fb4c3'}
            }
        '''

        if not self.hash_func:
            raise NotImplementedError('{} not implemented.'.format(self.hash))

        path = path.rstrip('/')

        hashpair = {}
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path,
                                             topdown=True,
                                             followlinks=followlinks):
                if '.snapshot' in dirs:
                    dirs.remove('.snapshot')
                if '~snapshot' in dirs:
                    dirs.remove('~snapshot')
                for f in files:
                    file_path = os.path.join(root, f)
                    hashpair[file_path] = list(
                        self._filehash(file_path)['allfileshash'].values())[0]
            hashdict = {
                'singledirhash': {
                    path: self._reduce_hash(hashpair.values())
                },
                'allfileshash': hashpair
            }
            return hashdict
        else:
            hashpair[path] = self._filehash(path)['allfileshash'].values()[0]
            hashdict = {
                'singledirhash': {
                    path: 'Its a file!'
                },
                'allfileshash': hashpair
            }
            return hashdict

    def _filehash(self, filepath):
        '''
        Create has for a file

        :param ``filepath`` : hash generate file path 

        :sample 
            _filehash('/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000')

        :returns: dict
            {'singledirhash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': 'Its a file'},
                'allfileshash': {'/tmp/cbs_backup_1533529247/000/000/000/000/cbt_output.0000': '9658fa063322ce28ba65ef0bbf6fb4c3'}
            }
        '''

        blocksize = 64 * 1024
        with open(filepath, 'rb') as fp:
            while True:
                data = fp.read(blocksize)
                if not data:
                    break
                self.hasher.update(data)
        hashdict = {
            'singledirhash': {
                filepath: 'Its a file!'
            },
            'allfileshash': {
                filepath: self.hasher.hexdigest()
            }
        }
        return hashdict

    def _reduce_hash(self, hashlist):
        '''
        Reduce hash for a directory to single hash

        :param ``hashlist``: list of all hash values
        '''

        for hashvalue in sorted(hashlist):
            self.hasher.update(hashvalue.encode('utf-8'))
        return self.hasher.hexdigest()
