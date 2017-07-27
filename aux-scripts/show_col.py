#!/usr/bin/python3

import sys
from   csv      import DictReader

ifn = sys.argv [1]

fields = dict \
    ( org_einheiten     = 0
    , schulkennzahlen   = 0
    , funktionen        = 0
    , benutzergruppen   = 0
    , aktiv_st_person   = 0
    , aktiv_a_person    = 0
    , aktiv_b_person    = 0
    , chipid_b          = 0
    , chipid_st         = 0
    , chipid_a          = 0
    , mirfareid_b       = 0
    , mirfareid_st      = 0
    , mirfareid_a       = 0
    , account_status_b  = 0
    , account_status_st = 0
    , account_status_a  = 0
    )


with open (ifn, 'r', encoding = 'utf-8') as ifile :
    dr = DictReader (ifile, delimiter = ';')
    for d in dr :
        for f in fields :
            if d [f] and fields [f] < 10 :
                print (d ['pk_uniqueid'], f, d [f])
                fields [f] += 1
