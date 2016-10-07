# -*- coding: utf-8 -*-
from __future__ import division, print_function
import inspect
from functools import wraps
import binfootprint
from . import PersistentDataStructure
from . import PersistentDataStructure_HDF5

def pd_func_cache(db_name=None, db_path='.', kind='sql', subdbkey=None, verbose=0):
    class pd_func_cache_decorator:
        def __init__(self, func):
            self.func = func
            self.__name__ = func.__name__
            self.__doc__ = func.__doc__
            self.verbose = verbose

            if db_name is None:
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
                print("subdbkey not set")
            else:
                if not isinstance(subdbkey, bytes):
                    self.subdbkey = binfootprint.dump(subdbkey)
                    print("subdbkey", subdbkey, "(converted to bytes)")
                else:
                    self.subdbkey = subdbkey
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


