from persistentdata import decorators
import logging
import argparse
import sys

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
                        default = '',
                        help    = "a path to include in sys.path")
    
    parser.add_argument('--debug',
                        action='store_true',
                        default=False,
                        help="enable debug mode")
    
    args = parser.parse_args()
    
    if args.debug:
        decorators.jm_log.setLevel(logging.DEBUG)
        
    if args.include != '':
        sys.path.append(args.include)
        
    
    decorators.Cache_Server._start_client(server  = args.hostname, 
                                          authkey = args.authkey, 
                                          port    = args.port, 
                                          nproc   = args.nproc, 
                                          nice    = args.nice, 
                                          show_statusbar_for_jobs = True, 
                                          show_counter_only       = True)