# -*- coding: utf-8 -*-
from __future__ import division, print_function
import inspect
from functools import wraps
import binfootprint
from . import PersistentDataStructure
from . import PersistentDataStructure_HDF5

import warnings
import logging
import multiprocessing as mp
import progression as progress

try:
    import jobmanager as jm
    from jobmanager.jobmanager import log as jm_log
    HAS_JOBMANAGER = True

    jm_log.setLevel(logging.WARNING)


    class Function_Client(jm.JobManager_Client):
        @staticmethod
        def func(arg, const_arg, c, m):
            f = const_arg['f']
            args, kwargs = arg
            return f(*args, **kwargs)

    class Cache_Server(jm.JobManager_Server):
        def __init__(self,
                     func,
                     db_name,
                     db_path,
                     subdbkey,
                     PDS,
                     authkey      = 'cache_dec',
                     port         = 42524,
                     msg_interval = 1):

            jm.JobManager_Server.__init__(self,
                                          authkey,
                                          const_arg    = {'f': func},
                                          port         = port,
                                          msg_interval = msg_interval,
                                          fname_dump   = None)
            self.func = func
            self.db_name = db_name
            self.db_path = db_path
            self.subdbkey = subdbkey
            self.PDS = PDS

        @staticmethod
        def _start_client(server,
                          authkey,
                          port,
                          nproc,
                          nice,
                          show_statusbar_for_jobs,
                          show_counter_only):

            client = Function_Client(server  = server,
                                     authkey = authkey,
                                     port    = port,
                                     nproc   = nproc,
                                     nice    = nice,
                                     show_statusbar_for_jobs = show_statusbar_for_jobs,
                                     show_counter_only       = show_counter_only,
                                     use_special_SIG_INT_handler = False)   # maps SIGINT to exit
            client.start()


        def process_new_result(self, arg, result):
            args, kwargs = arg
            callargs = inspect.getcallargs(self.func, *args, **kwargs)
            key = binfootprint.dump(callargs)
            with self.PDS(name=self.db_name, path=self.db_path) as db:
                if self.subdbkey is not None:
                    with db.getData(self.subdbkey, create_sub_data=True) as subdb:
                        subdb[key] = result
                else:
                    db[key] = result


        def put_arg(self, args, kwargs):
            callargs = inspect.getcallargs(self.func, *args, **kwargs)
            key = binfootprint.dump(callargs)

            with self.PDS(name=self.db_name, path=self.db_path) as db:
                if self.subdbkey is not None:
                    with db.getData(self.subdbkey, create_sub_data=True) as subdb:
                        if key in subdb:
                            return subdb[key]
                else:
                    if key in db:
                        return db[key]

            arg = (args, kwargs)
            jm.JobManager_Server.put_arg(self, arg)
            return None

except ImportError:
    warnings.warn("could not import jobmanager (pd_func_cache.call_list will not work)")
    HAS_JOBMANAGER = False


def pd_func_cache(db_name=None, db_path='.', kind='sql', subdbkey=None, verbose=0):
    class pd_func_cache_decorator:
        def __init__(self, func):
            self.func = func
            try:
                self.__name__ = func.__name__
            except AttributeError:                  # for example a 'functools.partial' object has no attribute __name__
                self.__name__ = None
            try:
                self.__doc__ = func.__doc__
            except AttributeError:
                self.__doc__ = None
            self.verbose = verbose

            if db_name is None:
                if self.__name__ is None:
                    raise ValueError("can not get db_name from func.__name (__name__ is None)")

                self.db_name = self.__name__
                if self.verbose > 0:
                    print("db_name ", db_name, "set from func.__name__")
            else:
                self.db_name = db_name
                if self.verbose > 0:
                    print("db_name ", db_name)

            self.db_path = db_path
            if verbose > 0:
                print("db_path ", db_path)

            if kind == 'sql':
                self.PDS = PersistentDataStructure
            elif kind == 'hdf5':
                self.PDS = PersistentDataStructure_HDF5
            else:
                raise ValueError("unknown kind '{}'".format(kind))

            if verbose > 0:
                print("kind    ", kind)

            if subdbkey is None:
                self.subdbkey = None
                if self.verbose > 0:
                    print("subdbkey not set")
            else:
                if not isinstance(subdbkey, bytes):
                    self.subdbkey = binfootprint.dump(subdbkey)
                    if self.verbose > 0:
                        print("subdbkey", subdbkey, "(converted to bytes)")
                else:
                    self.subdbkey = subdbkey
                    if self.verbose > 0:
                        print("subdbkey", subdbkey)

        def __call__(self, *args, **kwargs):
            callargs = inspect.getcallargs(self.func, *args, **kwargs)
            if self.verbose > 0:
                print("call with", callargs)
            key = binfootprint.dump(callargs)
            with self.PDS(name=self.db_name, path=self.db_path) as db:
                if self.subdbkey is not None:
                    with db.getData(self.subdbkey, create_sub_data=True) as subdb:
                        if key in subdb:
                            if self.verbose > 0:
                                print("key found in subdb")
                            return subdb[key]
                else:
                    if key in db:
                        if self.verbose > 0:
                            print("key found in db")
                        return db[key]

            if self.verbose > 0:
                print("key not found, crunch ...")
            # did not find any cached results
            value = self.func(*args, **kwargs)
            if self.verbose > 0:
                print("done!")
            with self.PDS(name=self.db_name, path=self.db_path) as db:
                if self.subdbkey is not None:
                    with db.getData(self.subdbkey, create_sub_data=True) as subdb:
                        if self.verbose > 0:
                            print("save value to subdb")
                        subdb[key] = value
                else:
                    if self.verbose > 0:
                        print("save value to db")
                    db[key] = value

            if self.verbose > 0:
                print("return value")
            return value

        def call_list(self, list_of_args, list_of_kwargs=None, nproc=0, nice=0, **kwargs_cache_server):
            def conv_list_args_kwargs(list_of_args, list_of_kwargs, i):
                if list_of_args is None:
                    args = tuple()
                else:
                    args = list_of_args[i]
                    if not isinstance(args, tuple):
                        args = (args,)

                if list_of_kwargs is None:
                    kwargs = dict()
                else:
                    kwargs = list_of_kwargs[i]
                return args, kwargs

            if not HAS_JOBMANAGER:
                raise RuntimeError("jobmanager was not imported, can not run 'call_list'")

            if list_of_args is not None:
                l = len(list_of_args)
            else:
                l = len(list_of_kwargs)

            res = [None] * l

            with Cache_Server(func     = self.func,
                              db_name  = self.db_name,
                              db_path  = self.db_path,
                              subdbkey = self.subdbkey,
                              PDS      = self.PDS,
                              **kwargs_cache_server) as cs:

                for i in range(l):
                    args, kwargs = conv_list_args_kwargs(list_of_args, list_of_kwargs, i)
                    r = cs.put_arg(args, kwargs)
                    if r is not None:
                        res[i] = r

                if cs.numjobs == 0:
                    cs.show_stat = False
                    return res

                cs.bring_him_up()
                p_client = mp.Process(target=Cache_Server._start_client,
                                      args = ('localhost',
                                              cs.authkey,
                                              cs.port,
                                              nproc,
                                              nice,
                                              False,    #show_statusbar_for_jobs,
                                              False))   #show_counter_only

                p_client.start()
                cs.join()
                progress.check_process_termination(p_client,
                                                   prefix  = 'local_client',
                                                   timeout = 2)

            for i in range(l):
                if res[i] is None:
                    args, kwargs = conv_list_args_kwargs(list_of_args, list_of_kwargs, i)
                    res[i] = cs.put_arg(args, kwargs)

            return res




        def clear_cache(self):
            with self.PDS(name=self.db_name, path=self.db_path) as db:
                if self.subdbkey is not None:
                    with db.getData(self.subdbkey, create_sub_data=True) as subdb:
                        subdb.clear()
                else:
                    db.clear()

        def has_key(self, *args, **kwargs):
            callargs = inspect.getcallargs(self.func, *args, **kwargs)
            key = binfootprint.dump(callargs)
            with self.PDS(name=self.db_name, path=self.db_path) as db:
                if self.subdbkey is not None:
                    with db.getData(self.subdbkey, create_sub_data=True) as subdb:
                        return key in subdb
                else:
                    return key in db

    return pd_func_cache_decorator


