'''
Created on 16 Aug 2016

@author: rlanda
'''

import sys

py3k = sys.version_info >= (3, 0)

if py3k:
    basestring = str
else:
    basestring = basestring
