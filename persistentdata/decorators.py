# -*- coding: utf-8 -*-
from __future__ import division, print_function
import inspect
from functools import wraps
import binfootprint
from . import PersistentDataStructure
from . import PersistentDataStructure_HDF5

def pd_func_cache(db_name, db_path='.', kind='sql', subdbkey=None, verbose=0):
    if kind == 'sql':
        PDS = PersistentDataStructure
    elif kind == 'hdf5':
        PDS = PersistentDataStructure_HDF5
    else:
        raise ValueError("unknown kind '{}'".format(kind))

    if verbose > 0:
        print("db_name ", db_name)
        print("db_path ", db_path)
        print("kind    ", kind)
        if subdbkey is None:
            print("subdbkey", subdbkey)
        else:
            if not isinstance(subdbkey, bytes):
                print("subdbkey", subdbkey, "(converted to bytes)")
                subdbkey = binfootprint.dump(subdbkey)
            else:
                print("subdbkey", subdbkey)

    class pd_func_cache_decorator:
        def __init__(self, func):
            self.func = func
            self.__name__ = func.__name__
            self.__doc__ = func.__doc__
            self.verbose = verbose

        def __call__(self, *args, **kwargs):
            callargs = inspect.getcallargs(self.func, *args, **kwargs)
            if self.verbose > 0:
                print("call with", callargs)
            key = binfootprint.dump(callargs)
            with PDS(name=db_name, path=db_path) as db:
                if subdbkey is not None:
                    with db.getData(subdbkey, create_sub_data=True) as subdb:
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
            with PDS(name=db_name, path=db_path) as db:
                if subdbkey is not None:
                    with db.getData(subdbkey, create_sub_data=True) as subdb:
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

        def clear_cache(self):
            with PDS(name=db_name, path=db_path) as db:
                if subdbkey is not None:
                    with db.getData(subdbkey, create_sub_data=True) as subdb:
                        subdb.clear()
                else:
                    db.clear()

        def has_key(self, *args, **kwargs):
            callargs = inspect.getcallargs(self.func, *args, **kwargs)
            key = binfootprint.dump(callargs)
            with PDS(name=db_name, path=db_path) as db:
                if subdbkey is not None:
                    with db.getData(subdbkey, create_sub_data=True) as subdb:
                        return key in subdb
                else:
                    return key in db

    return pd_func_cache_decorator


