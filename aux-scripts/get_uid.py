#!/usr/bin/python3

import sys
from   csv      import DictReader

ifn = sys.argv [1]

with open (ifn, 'r', encoding = 'utf-8') as ifile :
    dr = DictReader (ifile, delimiter = ';')
    for d in dr :
        print (d ['pk_uniqueid'])
