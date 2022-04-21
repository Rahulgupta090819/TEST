import paramiko
import traceback
from pathlib import *
from lib.api_utils import *


class FileOpManager(APIUtils):
    def __init__(self, hyperScalar, debugFile, **configs):
        super().__init__()
        self.debugFile    = debugFile
        self.port         = 22
        self.timeout      = 60
        self.vm           = None
        self.keyFile      = None
        self.volumes      = dict()
        self.drives       = dict()
        self.hyperScalar  = hyperScalar
        self.client       = configs['Client']
        self.project      = configs['Project']
        self.clientOS     = configs['clientOS']
        self.hostname     = configs['clientIP']
        self.username     = configs['username']
        self.password     = configs['password']
        self.azureSDK     = configs['azureSDK'] if 'azureSDK' in list(configs.keys()) else False
        self.vmName       = configs['vmName'] if 'vmName' in list(configs.keys()) else None
        self.rGroupName   = configs['resourceGroupName'] if 'resourceGroupName' in list(configs.keys()) else None
        self.logTimeZones = configs['logTimeZones']
        self.privateKey   = self.get_private_key()
        self.Status       = self.connect()

    def get_private_key(self):
        """
            Generate decoded private key for the given linux client connection
        """
        privateKey = None 

        if self.clientOS.lower() == 'linux':
            self.keyFile = '{0}/.ssh/{1}-{2}.pem'.format(str(Path.home()), os.uname()[1], self.hostname.replace('.', '-'))     
        else:
            return None

        try:
            with open(self.keyFile ,'r') as KF:
                KC = KF.read()

            if sys.version_info[0] < 3:
                import StringIO
                privateKey = StringIO.StringIO(KC)
            else:
                import io
                privateKey = io.StringIO(KC)

            return paramiko.RSAKey.from_private_key(privateKey)
        except Exception as errmsg:
            tbstr = str(traceback.format_exc())
            header = 'Warning: Seen traceback when tried to get private key for {0} client {1}/{2}'.format(
                    self.clientOS, self.project, self.client)
            self.print_stack_trace(header, tbstr, self.cs['roy'])       
            self.logme(header + '\n' + tbstr, console=False, debug=True)
            return None

    def connect(self):
        """
            SSHes the client with given credentials
        """
        Status = OrderedDict([
            ('Passed', False),
            ('Host', self.hostname),
            ('Port', self.port),
            ('Username', self.username),
            ('Password', '********'),
            ('KeyFile', self.keyFile),
            ('Timeout', self.timeout),
            ('Logs', None)
        ])

        self.vm = paramiko.SSHClient()
        self.vm.load_system_host_keys()
        self.vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        password = None if self.privateKey else self.password

        try:
            Status['Logs'] = self.vm.connect (
                hostname = self.hostname, 
                port = self.port, 
                username = self.username, 
                password = password,
                pkey = self.privateKey,
                timeout = self.timeout
            )
            if not Status['Logs']:
                Status['Logs'] = 'Info: Connected to host {0} successfully!'.format(self.hostname)
            Status['Passed'] = True
            cs = self.cs['wog']
            self.show_table('Info: Connection status of {0} client {1}/{2}:'.format(self.clientOS, self.project, self.client), Status, cs)
            self.logme(Status, console=False, debug=True, jf=True)
            #from scp import SCPClient
            #with SCPClient(self.vm.get_transport()) as scp:
            #    scp.put(self.get_smb_share_fix_script(), '.')
        except Exception as errorMessage:
            Status['Passed'] = False
            Status['Logs'] = 'Error: Connection to host {0} failed: {1}'.format(
                    self.hostname, str(errorMessage))
            cs = self.cs['wor']
            self.show_table('Error: Connection status of {0} client {1}/{2}:'.format(self.clientOS, self.project, self.client), Status, cs)
            self.logme(Status, console=False, debug=True, jf=True)
            sys.exit(0)

        return Status

    def get_fileop_time_stamp(self):
        return '{:%Y%b%d-%H%M%S}'.format(datetime.now())

    def get_time_now(self):
        return '{:%Y-%b-%d %H:%M:%S:%s}'.format(datetime.now())

    def get_elapsed_time(self, startTime):
        return '{:.2f} secs'.format(time.time() - startTime)

    def message_list_to_dict(self, outSource, messageList, Passed=True):
        """
            Converts given list of client command output to dictionary
        """

        if outSource == 'stdout':
            if messageList:
                message = str(messageList.pop()).replace('\n', '').strip()
                if message.isnumeric():
                    Output = int(message)
                    Passed = bool(Output)
                else:
                    Output = message
                    Passed = False
            else:
                return False, None

            if messageList:
                Output = OrderedDict([
                    ('Line-{0}'.format('{:03d}'.format(index)), str(line).strip().replace('\n','').replace('\r', '').strip())
                    for index, line in enumerate(messageList) if str(line).strip() 
                ])
            return Passed, Output
        else:
            if messageList:
                Output = OrderedDict([
                    ('Line-{0}'.format('{:03d}'.format(index)), str(line).strip().replace('\n','').replace('\r', '').strip())
                    for index, line in enumerate(messageList)
                ])
                return Passed, Output
            else:
                return Passed, None

    def fileop_mount_volume(self, **params):
        if self.clientOS.lower() == 'linux':
            return self.nfs_mount_volume(**params)
        else:
            return self.smb_mount_volume(**params)

    def check_and_set_dir_and_drive(self, protocol, **params):
        if 'volume' in list(params.keys()):
            volume = params.pop('volume')
            dirTimeStamp = params.pop('dirTimeStamp') if 'dirTimeStamp' in list(params.keys()) else True
            volumeName, vserver = self.get_volume_vserver(volume)
            if 'volumeName' not in list(params.keys()):
                params['volumeName'] = volumeName
            if protocol == 'NFS':
                volumePath = '{0}:/{1}'.format(vserver, volumeName)
                if dirTimeStamp:
                    dirName = '/tmp/{0}-{1}'.format(volumeName, self.get_fileop_time_stamp())
                else:
                    dirName = '/tmp/{0}'.format(volumeName)
                self.volumes[volumeName] = dirName
            else:
                self.drives[volumeName] = params.pop('drive')
        return params

    def fileop_unmount_volume(self, **params):
        if self.clientOS.lower() == 'linux':
            params = self.check_and_set_dir_and_drive('NFS', **params)
            return self.nfs_unmount_volume(**params)
        else:
            params = self.check_and_set_dir_and_drive('SMB', **params)
            return self.smb_unmount_volume(**params)

    def get_smb_share_fix_script(self):
        scriptPath = '/'.join(os.path.realpath(__file__).split('/')[:-2])
        return '{0}/utils/fix-smb-shares.bat'.format(scriptPath)

    def smb_mount_volume(self, **params):
        """
            Mounts SMB volume
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()
        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False

        volume = params['volume']
        protocol = params['protocol']
        drive = params['drive'] if 'drive' in list(params.keys()) else '*'
        vserver = params['vserver'] if 'vserver' in list(params.keys()) else None

        if isinstance(protocol, list):
            protocol = protocol[0]

        statusList = []

        volumeName, vserver = self.get_volume_vserver(volume, vserver)

        self.drives[volumeName] = drive

        volumePath = '{0}:/{1}'.format(vserver, volumeName)

        colon = '' if drive == '*' else ':'

        command = 'net use {0}{1} \\\\{2}\\{3} /persistent:yes /yes'.format(drive, colon, vserver, volumeName)
        if self.azureSDK:
            command = 'net use {0}{1} \\\\\\\\{2}\\\\{3}'.format(drive, colon, vserver, volumeName)

        Status = self.fileop_run(command, cmdDescription = 'Mount {0} volume {1}'.format(protocol, volumePath))

        if drive == '*':
            drive = re.match("Drive (.)\: is now connected", Status['Output']['Line-000'])[1]
            self.drives[volumeName] = drive

        statusList.append(Status)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        if Status['Passed']:
            Status['Passed'] = self.is_volume_mounted(drive, statusList)

        self.list_mounted_volumes(statusList)

        return self.get_status(startTime, startTimeStamp, statusList)

    def smb_unmount_volume(self, **params):
        """
            Unmounts SMB volume
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False

        volumeName = params['volumeName']
        drive = self.drives[volumeName]

        statusList = []

        command = 'net use {0}: /delete'.format(drive)

        Status = self.fileop_run(command, cmdDescription = 'Unmount volume {0} from drive {1}:'.format(volumeName, drive))

        statusList.append(Status)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        if Status['Passed']:
            Status['Passed'] = self.is_volume_unmounted(drive, statusList)

        self.list_mounted_volumes(statusList)

        return self.get_status(startTime, startTimeStamp, statusList) 

    def fileop_dd(self, **params):
        """
            Generates data files using dd command for linux NFS volumes
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False
        params = self.check_and_set_dir_and_drive('NFS', **params)

        statusList = []
        inputFile = params['if']
        outputFileName = params['of']
        blockSize = params['bs']
        count = params['count']

        volumeName = 'Unknown'
        if 'volume' in list(params.keys()):
            volumeName, vserver = self.get_volume_vserver(volume)
        elif 'volumeName' in list(params.keys()):
            volumeName = params['volumeName']

        filepath = self.volumes[volumeName] if volumeName in list(self.volumes.keys()) else ''
        outputFile = '{0}/{1}'.format(filepath, outputFileName)

        if not modeQuick:
            statusList.append(self.fileop_run('sudo ls -l {0}'.format(filepath), cmdDescription = 'List all files under {0}'.format(filepath)))

        command = 'sudo dd if={0} of={1} bs={2} count={3} 2>&1'.format(inputFile, outputFile, blockSize, count)
        cmdDescription = 'Writing file {0} for volume {1}'.format(outputFileName, volumeName)

        Status = self.fileop_run(command, cmdDescription=cmdDescription)
        statusList.append(Status)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        command = 'sudo ls -l {0} | grep {1}'.format(filepath, outputFileName, outputFile)
        cmdDescription = 'Checking whether file {0} is created'.format(outputFileName)

        Status = self.fileop_run(command, cmdDescription=cmdDescription)
        statusList.append(Status)

        statusList.append(self.fileop_run('sudo ls -l {0}'.format(filepath), cmdDescription = 'List all files under {0}'.format(filepath)))

        return self.get_status(startTime, startTimeStamp, statusList)

    def fileop_createtree(self, **params):
        """
            Generates data files using create_tree.pl script for linux NFS volumes
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        statusList = []
        path = params['path']
        FILE_CNT = params['FILE_CNT']
        NUM_RUNS = params['NUM_RUNS']
        DIR_CNT = params['DIR_CNT']
        DIR_DEPTH = params['DIR_DEPTH']
        FILE_SIZE = params['FILE_SIZE']
        OP_TYPE = params['OP_TYPE']

        params = self.check_and_set_dir_and_drive('NFS', **params)

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False

        volumeName = params.pop('volumeName') if 'volumeName' in list(params.keys()) else None

        volume = params.pop('volume') if 'volume' in list(params.keys()) else None

        if not volumeName and volume:

            volumeName, vserver = self.get_volume_vserver(volume)

        elif not volumeName and not volume:
            
            volumeName = 'Unknown'
        
        filePath = self.volumes[volumeName] if volumeName in list(self.volumes.keys()) else ''
        
        statusList.append(self.fileop_run('sudo ls -l {0}'.format(filePath), cmdDescription = 'List all files under {0}'.format(filePath)))

        if OP_TYPE == 'populate':

            command = 'sudo perl {0} OPERATION_TYPE=populate FILE_CNT={1} NUM_RUNS={2} DIR_CNT={3} DIR_DEPTH={4} FILE_SIZE={5} {6} 2>&1'.format(path, FILE_CNT, NUM_RUNS, DIR_CNT, DIR_DEPTH, FILE_SIZE, filePath)

        else:
            COMPRESSION_PCT = params['COMPRESSION_PCT']
            MOD_CREATE_PCT = params['MOD_CREATE_PCT']
            MOD_DELETE_PCT = params['MOD_DELETE_PCT']
            MOD_OVERWRITE_PCT = params['MOD_OVERWRITE_PCT']
            MOD_OVERWRITE_BY_PCT = params['MOD_OVERWRITE_BY_PCT']
            MOD_GROW_PCT = params['MOD_GROW_PCT']
            MOD_GROW_BY_PCT = params['MOD_GROW_BY_PCT']
            MOD_SHRINK_PCT = params['MOD_SHRINK_PCT']
            MOD_SHRINK_BY_PCT = params['MOD_SHRINK_BY_PCT']
            MOD_PUNCHHOLE_PCT = params['MOD_PUNCHHOLE_PCT']
            MOD_PUNCHHOLE_BY_PCT = params['MOD_PUNCHHOLE_BY_PCT']
            command = 'sudo perl {0} OPERATION_TYPE=modify FILE_CNT={1} NUM_RUNS={2} DIR_CNT={3} DIR_DEPTH={4} FILE_SIZE={5} COMPRESSION_PCT={6} MOD_CREATE_PCT={7} MOD_DELETE_PCT={8} MOD_OVERWRITE_PCT={9} MOD_OVERWRITE_BY_PCT={10} MOD_GROW_PCT={11} MOD_GROW_BY_PCT={12} MOD_SHRINK_PCT={13} MOD_SHRINK_BY_PCT={14} MOD_PUNCHHOLE_PCT={15} MOD_PUNCHHOLE_BY_PCT={16} {17} 2>&1'.format(path, FILE_CNT, NUM_RUNS, DIR_CNT, DIR_DEPTH, FILE_SIZE, COMPRESSION_PCT, MOD_CREATE_PCT, MOD_DELETE_PCT, MOD_OVERWRITE_PCT, MOD_OVERWRITE_BY_PCT, MOD_GROW_PCT, MOD_GROW_BY_PCT, MOD_SHRINK_PCT, MOD_SHRINK_BY_PCT, MOD_PUNCHHOLE_PCT, MOD_PUNCHHOLE_BY_PCT, filePath)

        cmdDescription = 'Writing file for volume {0}'.format(volumeName)

        Status = self.fileop_run(command, timeout=900, cmdDescription=cmdDescription)

        statusList.append(Status)
       
        command = 'sudo ls -l {0} | grep {1}'.format(filePath, 'root_dir')

        cmdDescription = 'Checking the root_dir'

        Status = self.fileop_run(command, cmdDescription=cmdDescription)

        statusList.append(Status)
        
        statusList.append(self.fileop_run('sudo ls -l {0}'.format(filePath), cmdDescription = 'List all files under {0}'.format(filePath)))

        return self.get_status(startTime, startTimeStamp, statusList)


    def fileop_az_run(self, **params):
        """
            Executes any command in clients
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        statusList = []
        volumeName = params['volumeName']
        Mode = params['Mode']
        SVMIP = params['IP']
        Version = params['Version']
        command = 'cd /var/tmp && ./fio.sh {0}:{1}:{3} {2} 2>&1'.format(volumeName, SVMIP, Mode, Version)

        cmdDescription = 'Calling fio.sh file for volume {0} with mode{1} for version {2}'.format(volumeName, Mode, Version)

        Status = self.fileop_run(command, cmdDescription=cmdDescription)
        statusList.append(Status)
        return Status

    def fileop_python_az(self, **params):
        """
            Executes any command in clients
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        statusList = []
        volumeName = params['volumeName']
        Mode = params['Mode']
        SVMIP = params['IP']
        Version = params['Version']
        if 'nfs' in Version.lower():

            command = 'cd /var/tmp && python3 python_fileops.py -v {0} -ip {1} -p {3} -m {2} 2>&1'.format(volumeName, SVMIP, Mode, Version)
            cmdDescription = 'Calling python_fileops.py file for volume {0} with mode{1} for version {2}'.format(volumeName, Mode, Version)
        else:
            command = 'cd C:\\Users\\azureuser && py python_fileops.py -v {0} -ip {1} -p {3} -m {2} 2>&1'.format(volumeName, SVMIP, Mode, Version)
            cmdDescription = 'Calling python_fileops.py file for volume {0} with mode{1} for version {2}'.format(volumeName, Mode, Version)

        Status = self.fileop_run(command, cmdDescription=cmdDescription)
        statusList.append(Status)
        return Status


    def fileop_fsutil(self, **params):
        """
            Generates data files using fsutil command for windows SMB volumes
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False
        params = self.check_and_set_dir_and_drive('SMB', **params)

        statusList = []
        filename = params['filename']
        length = params['length']
        volumeName = params['volumeName']
        drive = self.drives[volumeName]

        if not modeQuick:
            statusList.append(self.fileop_run('dir {0}:\\'.format(drive), cmdDescription='List all files under drive {0}:'.format(drive)))

        command = 'del {0}:\\{1} 2>&1 && fsutil file createNew {0}:\\{1} {2}'.format(drive, filename, length)
        cmdDescription = 'Writing data on drive {0}: for volume {1}'.format(drive, volumeName)

        Status = self.fileop_run(command, cmdDescription=cmdDescription)
        statusList.append(Status)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        command = 'dir {0}: | findstr {1}'.format(drive, filename)
        cmdDescription = 'Checking whether file {0} is created'.format(filename)

        Status = self.fileop_run(command, cmdDescription=cmdDescription)
        statusList.append(Status)

        statusList.append(self.fileop_run('dir {0}:\\'.format(drive), cmdDescription='List all files under drive {0}:'.format(drive)))

        return self.get_status(startTime, startTimeStamp, statusList)

    def fileop_run(self, command, timeout=300, cmdDescription='', reverseCheck=False, verify=None):
        """
            Executes any command in clients
        """
        command = command.strip()

        Status = OrderedDict([
            ('Passed', False),
            ('Description', cmdDescription),
            ('StartTime', self.get_time_stamps()),
            ('EndTime', 0),
            ('Elapsed', 0),
            ('Timeout', timeout),
            ('Command', command),
            ('Output', None),
            ('Error', None)
        ])

        echopass = '&& echo 1 || echo 0'
        if reverseCheck:
            echopass = '&& echo 0 || echo 1'

        if self.clientOS.lower() == 'linux':
            if command.startswith('sudo') and not self.azureSDK:
                sudopass = 'sshpass -p {0}'.format(self.password)
                command = "{0} {1} {2}".format(sudopass, command, echopass)
            else:
                command = "{0} {1}".format(command, echopass)
        else:
            command = "{0} {1}".format(command, echopass)
            if self.azureSDK:
                command = "cmd /c \\\"{0}\\\"".format(command)

        if self.azureSDK:
            if self.clientOS.lower() == 'linux':
                cleanCommandId = 'RemoveRunCommandLinuxExtension 2> /dev/null'
                runCommandId = 'Runshellscript'
            else:
                cleanCommandId = 'RemoveRunCommandWindowsExtension 2> $null'
                runCommandId = 'RunPowerShellScript'
            cleanCommand = "az vm run-command invoke -g {0} -n {1} --command-id {2}".format(
                    self.rGroupName, self.vmName, cleanCommandId)
            command = "{0} ; az vm run-command invoke -g {1} -n {2} --command-id {3} --query value[*].message --script \"{4}\"".format(
                    cleanCommand, self.rGroupName, self.vmName, runCommandId, command)

            #print('Command: {}'.format(command))
        try:
            startTime = time.time()
            stdin, stdout, stderr = self.vm.exec_command(command, timeout = timeout)

            if self.azureSDK and stdout:
                Output = stdout.readlines()
                #print('Unformatted Azure Output: {}'.format(Output))
                if self.clientOS.lower() == 'linux':
                    azureOutput = re.findall(
                            ".*\[stdout\](.*)\[stderr\]", Output[1].strip())[0].replace('\\n', '\n').strip().strip('"').split('\n')
                    azureError = re.findall("\[stderr\](.*)\"", Output[1].strip())[0].replace('\\n', '\n').strip().strip('"').split('\n')
                else:
                    azureOutput = re.findall("\"(.*)\"", Output[1].strip())[0].replace('\\n', '\n').strip().strip('"').split('\n')
                    azureError = re.findall("\"(.*)\"", Output[2].strip())[0].replace('\\n', '\n').strip().strip('"').split('\n')
                azureError = [item for item in azureError if item]
                #print('Formatted Azure STDOUT: {}'.format(azureOutput))
                #print('Formatted Azure STDERR: {}'.format(azureError))
                Status['Passed'], Status['Output'] = self.message_list_to_dict('stdout', azureOutput, True)
                if verify:
                    Output = sum(Status['Output'].items())
                    if verify in Output:
                        Status['Passed'], Status['Output'] = self.message_list_to_dict('stdout', azureOutput, True)
                    else:
                        Status['Passed'], Status['Output'] = self.message_list_to_dict('stdout', azureOutput,
                                                                                       Status['Passed'])
                        Status['Output'] = verify + " is not in " + Status['Output']

                if azureError:
                    Status['Passed'], Status['Error'] = self.message_list_to_dict('stderr', azureError, Status['Passed'])
            else:
                Status['Passed'] = False
                if stdout:
                    Output = stdout.readlines()
                    if verify:
                        if verify in Output[0]:
                            Status['Passed'], Status['Output'] = self.message_list_to_dict('stdout', Output, True)
                        else:
                            Status['Output'] = verify + " is not in " + Output[0]
                    else:
                        Status['Passed'], Status['Output'] = self.message_list_to_dict('stdout', Output, True)

                if stderr:
                    Error = stderr.readlines()  
                    Status['Passed'], Status['Error'] = self.message_list_to_dict('stderr', Error, Status['Passed'])

        except Exception as errorMessage:
            Status['Error'] = str(errorMessage)

        Status['Elapsed'] = self.get_elapsed_time(startTime)
        Status['EndTime'] = self.get_time_stamps()

        return Status

    def is_directory_exists(self, dirName, statusList):
        command = '[ -d {0} ]'.format(dirName)

        Status = self.fileop_run(command, cmdDescription = 'Check directory {0} is exists'.format(dirName))

        statusList.append(Status)

        return Status['Passed']

    def is_directory_not_exists(self, dirName, statusList):
        command = '[ -d {0} ]'.format(dirName)

        Status = self.fileop_run(command, cmdDescription = 'Check directory {0} not exists'.format(dirName), reverseCheck=True)

        statusList.append(Status)

        return Status['Passed']

    def make_directory(self, dirName, statusList, modeQuick):
        command = 'sudo mkdir -p {0}'.format(dirName)

        Status = self.fileop_run(command, cmdDescription = 'Create directory {0}'.format(dirName))

        statusList.append(Status)

        if Status['Passed'] and not modeQuick:
            return self.is_directory_exists(dirName, statusList)

        return Status['Passed']

    def change_mode(self, dirName, statusList, fileMode=777):
        command = 'sudo chmod {0} {1}'.format(fileMode, dirName)

        Status = self.fileop_run(command, cmdDescription = 'Change access mode to {0} for directory {1}'.format(fileMode, dirName))

        statusList.append(Status)

        return Status['Passed']

    def remove_directory(self, dirName, statusList):
        command = 'sudo rmdir {0}'.format(dirName)

        Status = self.fileop_run(command, cmdDescription = 'Delete directory {0}'.format(dirName))

        statusList.append(Status)

        if Status['Passed']:
            return not self.is_directory_not_exists(dirName, statusList)

        return Status['Passed']

    def list_mounted_volumes(self, statusList):
        if self.clientOS.lower() == 'linux':
            command = 'sudo df -h 2>&1'
        else:
            command = 'net use'

        Status = self.fileop_run(command, cmdDescription = 'List all the mounted volumes')

        if self.clientOS.lower() == 'linux' and not Status['Passed']:
            Status['Passed'] = True

        statusList.append(Status)

        return Status['Passed']

    def is_volume_mounted(self, dirName, statusList, volumeName=None):
        if self.clientOS.lower() == 'linux':
            command = "sudo df -h 2>&1 | grep '{0} .* {1} *$'".format(volumeName, dirName)
            vtype = 'volume'
        else: 
            dirName = '{0}:'.format(dirName)
            command = 'net use | findstr {0}'.format(dirName)  
            vtype = 'drive'

        Status = self.fileop_run(command, cmdDescription = 'Check {0} {1} is mounted'.format(vtype, dirName))

        statusList.append(Status)

        return Status['Passed']

    def is_volume_unmounted(self, dirName, statusList, volumeName=None):
        if self.clientOS.lower() == 'linux':
            command = "sudo df -h 2>&1 | grep '{0} .* {1} *$'".format(volumeName, dirName)
            vtype = 'volume'
        else:
            dirName = '{0}:'.format(dirName)
            command = 'net use | findstr {0}'.format(dirName)
            vtype = 'drive'

        Status = self.fileop_run(command, cmdDescription = 'Check {0} {1} is unmounted'.format(vtype, dirName), reverseCheck=True)

        statusList.append(Status)

        return Status['Passed']

    def get_volume_vserver(self, volume, vserver=None):
        """
            Collects creationToken and vserver of the given volume
        """
        volumeName = None

        if 'GetLogs' in list(volume.keys()):
            volume = volume['GetLogs']['Response']
        else:
            volume = volume['Response']

        if self.hyperScalar == 'ANF':
            if not vserver:
                vserver = volume['properties']['mountTargets'][0]['ipAddress']
            volumeName = volume['properties']['creationToken']
        else:
            if not vserver:
                vserver = volume['mountPoints'][0]['server']
            volumeName = volume['creationToken']

        return volumeName, vserver

    def nfs_mount_volume(self, **params):
        """
            Mounts NFS volume for the given NFS version v3 or v4
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False

        volume = params['volume']
        protocol = params['protocol']
        ignoreFileMode = params.pop('ignoreFileMode') if 'ignoreFileMode' in list(params.keys()) else False
        fileMode = params.pop('fileMode') if 'fileMode' in list(params.keys()) else 777
        dirTimeStamp = params.pop('dirTimeStamp') if 'dirTimeStamp' in list(params.keys()) else True

        statusList = []

        if isinstance(protocol, list):
            protocol = protocol[0]

        if str(protocol).lower() == 'nfsv3':
            vers = 3
        elif str(protocol).lower() in ('nfsv4', 'nfsv4.1'):
            vers = 4.1

        volumeName, vserver = self.get_volume_vserver(volume)

        volumePath = '{0}:/{1}'.format(vserver, volumeName)

        if dirTimeStamp:
            dirName = '/tmp/{0}-{1}'.format(volumeName, self.get_fileop_time_stamp())
        else:
            dirName = '/tmp/{0}'.format(volumeName)

        self.volumes[volumeName] = dirName

        self.make_directory(dirName, statusList, modeQuick)

        if not ignoreFileMode:
            self.change_mode(dirName, statusList, fileMode)

        options = str(params.pop('options')) + ',' if 'options' in list(params.keys()) else ''

        command = 'sudo mount -t nfs -o {0}rw,hard,rsize=65536,wsize=65536,vers={1},tcp {2} {3}'.format(options, vers, volumePath, dirName)

        Status = self.fileop_run(command, cmdDescription = 'Mount {0} volume {1} in directory {2}'.format(protocol, volumePath, dirName))

        statusList.append(Status)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        if Status['Passed']:
            Status['Passed'] = self.is_volume_mounted(dirName, statusList, volumeName)

        self.list_mounted_volumes(statusList)

        return self.get_status(startTime, startTimeStamp, statusList)

    def get_status(self, startTime, startTimeStamp, statusList):
        """
            Analyze and collects the final status of all commands
        """
        Status = OrderedDict([
            ('Passed', True),
            ('StartTime', startTimeStamp),
            ('EndTime', self.get_time_stamps()),
            ('Elapsed', self.get_elapsed_time(startTime)),
            ('Logs', dict())
        ])

        for status in statusList:
            if not status['Passed']:
                Status['Passed'] = status['Passed']
                break

        Status['Logs'] = OrderedDict([
            ('Log-{0}'.format(index), status) 
            for index, status in enumerate(statusList)
        ])

        return Status

    def fileop_fio(self, **params):
        """
            Generates data file using fio command in linux for NFS volumes
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False
        params = self.check_and_set_dir_and_drive('NFS', **params)

        statusList = [] 

        volumeName = params.pop('volumeName') if 'volumeName' in list(params.keys()) else None
        volume = params.pop('volume') if 'volume' in list(params.keys()) else None

        if not volumeName and volume:
            volumeName, vserver = self.get_volume_vserver(volume)
        elif not volumeName and not volume:
            volumeName = 'Unknown'

        filePath = self.volumes[volumeName] if volumeName in list(self.volumes.keys()) else ''

        iteration = params.pop('iteration') if 'iteration' in list(params.keys()) else 1
        interval = params.pop('interval') if 'interval' in list(params.keys()) else 1  
        filename = params['filename'] if 'filename' in list(params.keys()) else 'sample-data'

        params['name'] = params['name'] if 'name' in list(params.keys()) else 'sysqa-data'
        params['time_based'] = int(params['time_based']) if 'time_based' in list(params.keys()) else 0
        params['group_reporting'] = int(params['group_reporting']) if 'group_reporting' in list(params.keys()) else 1
        params['filename'] = '{0}/{1}'.format(filePath, filename)

        #filePath = self.volumes[volumeName]

        if not modeQuick:
            statusList.append(self.fileop_run('sudo ls -l {0}'.format(filePath), cmdDescription = 'List all files under {0}'.format(filePath)))

        fioCommand = "sudo fio --direct=1 --ioengine=libaio --eta-newline=1 --fallocate=none"

        command = "{0} {1}".format(fioCommand, ' '.join(['--{0}={1}'.format(k,v) for k,v in params.items()]))

        #statusList.append(self.fileop_run('sudo ls -l {0}'.format(filePath), cmdDescription = 'List all files under {0}'.format(filePath)))

        for index in range(iteration):
            statusList.append(self.fileop_run(command, timeout=None, cmdDescription = 'Run fio command under {0}'.format(filePath)))
            time.sleep(interval)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        command = 'sudo ls -l {0} | grep {1}'.format(filePath, filename, params['filename'])
        cmdDescription = 'Checking whether file {0} is created'.format(filename)
        statusList.append(self.fileop_run(command, cmdDescription=cmdDescription))

        statusList.append(self.fileop_run('sudo ls -l {0}'.format(filePath), cmdDescription = 'List all files under {0}'.format(filePath)))

        return self.get_status(startTime, startTimeStamp, statusList)

    def nfs_unmount_volume(self, **params):
        """
            Unmount given NFS volume
        """
        startTime = time.time()
        startTimeStamp = self.get_time_stamps()

        modeQuick = params.pop('modeQuick') if 'modeQuick' in list(params.keys()) else False

        volumeName = 'Unknown'
        if 'volume' in list(params.keys()):
            volumeName, vserver = self.get_volume_vserver(volume)
        elif 'volumeName' in list(params.keys()):
            volumeName = params['volumeName']

        statusList = []

        dirName = self.volumes[volumeName] if volumeName in list(self.volumes.keys()) else ''

        command = 'sudo umount -f {0}'.format(dirName)

        Status = self.fileop_run(command, cmdDescription = 'Unmount volume {0} from directory {1}'.format(volumeName, dirName))

        statusList.append(Status)

        if modeQuick:
            return self.get_status(startTime, startTimeStamp, statusList)

        if Status['Passed']:
            Status['Passed'] = self.is_volume_unmounted(dirName, statusList, volumeName)

        self.remove_directory(dirName, statusList)

        self.list_mounted_volumes(statusList)

        return self.get_status(startTime, startTimeStamp, statusList)

    def close(self):
        try:
            self.vm.close()
        except:
            pass

    def __del__(self):
        self.close()
                            

