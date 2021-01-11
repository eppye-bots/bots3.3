#!/usr/bin/env python
from __future__ import print_function
import sys
import os
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer
import time
import subprocess
import threading
import botsinit
import botslib
import botsglobal


#-------------------------------------------------------------------------------
PRIORITY = 0
JOBNUMBER = 1
TASK = 2

class Jobqueue(object):
    ''' handles the jobqueue.
        methodes can be called over xmlrpc (except the methods starting with '_')
    '''
    def __init__(self,logger):
        self.jobqueue = []       # list of jobs. in jobqueue are jobs are: (priority,jobnumber,task)
        self.jobcounter = 0      # to assign unique sequential job-number
        self.logger = logger

    def addjob(self,task,priority):
        #canonize task (to better find duplicates)??. Is dangerous, as non-bots-tasks might be started....
        #first check if job already in queue
        for job in self.jobqueue:
            if job[TASK] == task:
                if job[PRIORITY] != priority:   #change priority. is this useful?
                    job[PRIORITY] = priority
                    self.logger.info('Duplicate job, changed priority to %(priority)s: %(task)s',{'priority':priority,'task':task})
                    self._sort()
                    return 0        #zero or other code??
                else:
                    self.logger.info('Duplicate job not added: %(task)s',{'task':task})
                    return 4
        #add the job
        self.jobcounter += 1
        self.jobqueue.append([priority, self.jobcounter,task])
        self.logger.info('Added job %(job)s, priority %(priority)s: %(task)s',{'job':self.jobcounter,'priority':priority,'task':task})
        self._sort()
        return 0

    def clearjobq(self):
        del self.jobqueue[:]
        self.logger.info('Job queue cleared.')
        return 0

    def getjob(self):
        if len(self.jobqueue):
            return self.jobqueue.pop()
        return 0

    def _sort(self):
        self.jobqueue.sort(reverse=True)
        self.logger.debug('Job queue changed. New queue: %(queue)s',{'queue':''.join(['\n    ' + repr(job) for job in self.jobqueue])})

#-------------------------------------------------------------------------------
def action_when_time_out(logger,maxruntime,jobnumber,task_to_run):
    logger.error('Job %(job)s exceeded maxruntime of %(maxruntime)s minutes',{'job':jobnumber,'maxruntime':maxruntime})
    botslib.sendbotserrorreport('[Bots Job Queue] - Job exceeded maximum runtime',
                                'Job %(job)s exceeded maxruntime of %(maxruntime)s minutes:\n %(task)s' % {'job':jobnumber,'maxruntime':maxruntime,'task':task_to_run})

#-------------------------------------------------------------------------------
def launcher(logger,port,lauchfrequency,maxruntime):
    DEVNULL = open(os.devnull, 'wb')
    xmlrpcclient = xmlrpclib.ServerProxy('http://localhost:' + unicode(port))
    maxseconds = maxruntime*60
    time.sleep(3)   #allow jobqserver to start
    nr_runs_NOK = 0
    while True:
        try:
            time.sleep(lauchfrequency)
            job = xmlrpcclient.getjob()

            if job:       #0 means nothing to launch
                result = None
                priority,jobnumber,task_to_run = job
                logger.info('Starting job %(job)s',{'job':jobnumber})
                starttime = time.time()
                process = subprocess.Popen(task_to_run,stdout=DEVNULL,stderr=DEVNULL)
                timer = threading.Timer(maxseconds, action_when_time_out,args=(logger,maxruntime,jobnumber,task_to_run))
                timer.start()
                result = process.wait()
                timer.cancel()
                time_taken = time.time() - starttime
                logger.info('Finished job %(job)s, elapsed time %(time_taken)s, result %(result)s',{'job':jobnumber,'time_taken':time_taken,'result':result})
                nr_runs_NOK = 0
        except Exception as msg:
            nr_runs_NOK += 1
            logger.error('Error occured in the bots-jobqueueserver: %(msg)s',{'msg':msg})
            botslib.sendbotserrorreport('[Bots Job Queue] Error in bots-jobqueueserver',
                                        'An error occured in the bots-jobqueueserver: %(msg)s' % {'msg':msg})
            if nr_runs_NOK >= 10:
                logger.error('More than 10 consecutive errors in the bots-jobqueueserver, shutting down now')
                botslib.sendbotserrorreport('[Bots Job Queue] bots-jobqueueserver has stopped',
                                            'More than 10 consecutive errors occured in the bots-jobqueueserver, so jobqueue-server is stopped now.'})
                sys.exit(1)




def start():
    #NOTE: bots directory should always be on PYTHONPATH - otherwise it will not start.
    #***command line arguments**************************
    usage = '''
    This is "%(name)s" version %(version)s, part of Bots open source edi translator (http://bots.sourceforge.net).
    Server program that ensures only a single bots-engine runs at any time, and no engine run requests are
    lost/discarded. Each request goes to a queue and is run in sequence when the previous run completes.
    Use of the job queue is optional and must be configured in bots.ini (jobqueue section, enabled = True).
    Usage:
        %(name)s  -c<directory>
    Options:
        -c<directory>   directory for configuration files (default: config).

    '''%{'name':os.path.basename(sys.argv[0]),'version':botsglobal.version}
    configdir = 'config'
    for arg in sys.argv[1:]:
        if arg.startswith('-c'):
            configdir = arg[2:]
            if not configdir:
                print('Error: configuration directory indicated, but no directory name.')
                sys.exit(1)
        else:
            print(usage)
            sys.exit(0)
    #***end handling command line arguments**************************
    botsinit.generalinit(configdir)     #find locating of bots, configfiles, init paths etc.
    if not botsglobal.ini.getboolean('jobqueue','enabled',False):
        print('Error: bots jobqueue cannot start; not enabled in %s/bots.ini', configdir)
        sys.exit(1)
    process_name = 'jobqueue'
    logger = botsinit.initserverlogging(process_name)
    logger.log(25,'Bots %(process_name)s started.',{'process_name':process_name})
    logger.log(25,'Bots %(process_name)s configdir: "%(configdir)s".',{'process_name':process_name,'configdir':botsglobal.ini.get('directories','config')})
    port = botsglobal.ini.getint('jobqueue','port',28082)
    logger.log(25,'Bots %(process_name)s listens for xmlrpc at port: "%(port)s".',{'process_name':process_name,'port':port})

    #start launcher thread
    lauchfrequency = botsglobal.ini.getint('jobqueue','lauchfrequency',5)
    maxruntime = botsglobal.ini.getint('settings','maxruntime',60)
    launcher_thread = threading.Thread(name='launcher', target=launcher, args=(logger,port,lauchfrequency,maxruntime))
    launcher_thread.daemon = True
    launcher_thread.start()
    logger.info('Jobqueue launcher started.')

    #the main thread is the xmlrpc server: all adding, getting etc for jobqueue is done via xmlrpc.
    logger.info('Jobqueue server started.')
    server = SimpleXMLRPCServer(('localhost', port),logRequests=False)
    server.register_instance(Jobqueue(logger))
    try:
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

    sys.exit(0)


if __name__ == '__main__':
    start()
