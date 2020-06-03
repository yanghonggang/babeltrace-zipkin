#!/usr/bin/env python
# -*- coding: utf-8 -*
# vi:set tw=0 ts=4 sw=4 nowrap fdm=indent
import sys 
import pandas

index = int(sys.argv[2])
col = pandas.read_table(sys.argv[1], header=None, delim_whitespace=True)
print ('max=%d\nmin=%d\navg=%d\nstd=%d\n' % (col.max()[index],
       col.min()[index], col.mean()[index], col.std()[index]))
