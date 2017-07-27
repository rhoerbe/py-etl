#!/usr/bin/python3

import sys
from   csv      import DictReader, DictWriter

ifn = sys.argv [1]
uid = dict.fromkeys (sys.argv [2].split (','))

wr = None
with open (ifn, 'r', encoding = 'utf-8') as ifile :
    dr = DictReader (ifile, delimiter = ';')
    for d in dr :
        if wr is None :
            wr = DictWriter (sys.stdout, dr.fieldnames, delimiter = ';')
            wr.writeheader ()
        if d ['pk_uniqueid'] in uid :
            wr.writerow (d)
