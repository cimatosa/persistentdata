from persistentdata import decorators
import logging
import argparse
import sys
import datetime
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='starts additional clients for the persistent data function cache')
    
    parser.add_argument('--hostname', '-hn',
                        type    = str,
                        default = None,
                        help    = "hostname where the server runs")
    
    parser.add_argument('--authkey', '-a',
                        type    = str,
                        default = decorators.DEFAULT_AUTHKEY,
                        help    = "authkey for connecting to the server")
    
    parser.add_argument('--port', '-p',
                        type    = int,
                        default = decorators.DEFAULT_PORT,
                        help    = "port to use for connection")
    
    parser.add_argument('--nproc', '-np',
                        type    = int,
                        default = 0,
                        help    = "tells the client how many subprocesses to spawn")

    parser.add_argument('--nice', '-ni',
                        type    = int,
                        default = 19,
                        help    = "the niceness of the subprocesses")
    
    parser.add_argument('--include', '-i',
                        type    = str,
                        nargs   = '*',
                        default = '',
                        help    = "a path to include in sys.path")
    
    parser.add_argument('--debug',
                        action='store_true',
                        default=False,
                        help="enable debug logging mode")
    
    parser.add_argument('--info',
                        action='store_true',
                        default=False,
                        help="enable info logging mode")
    
    parser.add_argument('--logtofile',
                        action='store_true',
                        default=False,
                        help="write log to file")
       
    args = parser.parse_args()
        
    if args.info:
        decorators.jm_log.setLevel(logging.INFO)
       
    if args.debug:
        decorators.jm_log.setLevel(logging.DEBUG)
        
    for i in args.include:
        sys.path.append(i)
        
    if args.logtofile:
        fname = "{}_pid_{}.out".format(datetime.datetime.now().isoformat(), os.getpid())
        fhandl = logging.FileHandler(fname)
        fhandl.setLevel(logging.DEBUG)
        l = logging.getLogger('jobmanager.jobmanager')
        l.addHandler(fhandl)
    
    decorators.Cache_Server._start_client(server  = args.hostname, 
                                          authkey = args.authkey, 
                                          port    = args.port, 
                                          nproc   = args.nproc, 
                                          nice    = args.nice, 
                                          show_statusbar_for_jobs = True, 
                                          show_counter_only       = True)