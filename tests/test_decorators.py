#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import sys
from os.path import abspath, dirname, split, exists
# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

from persistentdata.decorators import pd_func_cache
import time

DELTA_T = 2

@pd_func_cache(db_name='f', verbose=1)
def f(x, dt=0):
    """f doc"""
    time.sleep(dt)
    return x**2

@pd_func_cache(db_name='f', verbose=1, subdbkey = {'funcname': 'g'})
def g(x):
    """f doc"""
    return x**3

@pd_func_cache(verbose=1, subdbkey = b'\xff', kind='hdf5')
def h(x):
    """f doc"""
    return x**4

def test_call_dec_f():
    f.clear_cache()

    t0 = time.time()
    f(3, DELTA_T)
    t1 = time.time()
    assert t1 - t0 > DELTA_T, "{}".format(t1-t0)

    t0 = time.time()
    f(3, DELTA_T)
    t1 = time.time()
    assert t1 - t0 < DELTA_T

    assert f.__name__ == 'f'
    assert f.__doc__ == 'f doc'

def test_call_dec_f_and_g():
    f.clear_cache()
    g.clear_cache()
    assert not f.has_key(3)
    assert not g.has_key(3)
    f(3)
    assert f.has_key(3)
    assert not g.has_key(3)
    f(3)

    g(4)
    assert not f.has_key(4)
    assert g.has_key(4)
    g(4)

def test_call_dec_h():
    h.clear_cache()
    assert not h.has_key(3)
    h(3)
    assert h.has_key(3)
    h(3)


def test_unknown_kind():
    try:
        @pd_func_cache(kind = 'hdf7')
        def a(x):
            pass
    except ValueError as e:
        print("caught", e)
        print("that is ok")

if __name__ == "__main__":
    # test_call_dec_f()
    # test_call_dec_f_and_g()
    # test_call_dec_h()
    test_unknown_kind()
