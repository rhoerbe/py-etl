#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import base64
from   csv      import DictReader, DictWriter
from   random   import SystemRandom
from   binascii import hexlify

ifn = sys.argv [1]
ofn = sys.argv [1] + '.anonymized'

text_hi = "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ "
text_lo = "abcdefghijklmnopqrstuvwxyzäöüß "
pwchr   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567899"
textmap = { 'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 's'
          , 'Ä': 'A', 'Ö': 'O', 'Ü': 'U'
          }

rand = SystemRandom ()

def randname (l) :
    result = []
    hilen  = len (text_hi)
    lolen  = len (text_lo)
    result.append (text_hi [rand.randint (0, hilen - 1)])
    for k in range (l - 1) :
        result.append (text_lo [rand.randint (0, lolen - 1)])
    return ''.join (result)
# end def randname

def randascii (l) :
    return ''.join (textmap.get (x, x) for x in randname (l))
# end def randascii

def randstr (l) :
    return os.urandom (l)
# end def randstr

def randpw (l) :
    result = []
    for k in range (l) :
        result.append (pwchr [rand.randint (0, len (pwchr) - 1)])
    return ''.join (result)
# end def randpw

class Randnum (object) :
    """ Returns a number from the given range
    """
    def __init__ (self, start, end) :
        self.start = start
        self.end   = end
    # end def __init__

    def __iter__ (self) :
        while True :
            yield rand.randint (self.start, self.end)
    # end def __iter__

# end class Randnum

#                              random range                float
randnums = dict \
    ( PM_SAP_PERSONALNUMMER = (iter (Randnum (1111111, 99999999)),  False)
    , PERSON_NR             = (iter (Randnum (11111, 999999)),      True)
    , ST_PERSON_NR          = (iter (Randnum (11111, 999999)),      True)
    , IDENT_NR              = (iter (Randnum (11111, 999999)),      True)
    , MATRIKELNUMMER        = (iter (Randnum (11111111, 99999999)), False)
    )

hexnums = dict \
    ( PERSON_NR_OBF    = 8
    , ST_PERSON_NR_OBF = 8
    , MIRFAREID_B      = 4
    , MIRFAREID_ST     = 4
    , MIRFAREID_A      = 4
    )

usercount = 0
with open (ifn, 'r', encoding = 'utf-8') as ifile :
    dr = DictReader (ifile, delimiter = ';')
    wr = None
    with open (ofn, 'w', encoding = 'utf-8') as ofile :
        for d in dr :
            if wr is None :
                wr = DictWriter (ofile, dr.fieldnames, delimiter = ';')
                wr.writeheader ()
            for k in 'VORNAME', 'NACHNAME' :
                if d [k] :
                    d [k] = randname (len (d [k]))
            for k in 'EMAILADRESSE_B', 'EMAILADRESSE_ST' :
                if d [k] :
                    if '@' in d [k] :
                        email, domain = d [k].split ('@')
                    else :
                        email = d [k]
                        domain = 'example.com'
                    d [k] = randascii (len (email)) + '@' + domain
            if d ['BENUTZERNAME'] :
                np = []
                for k in 'VORNAME', 'NACHNAME' :
                    np.append \
                        (''.join (textmap.get (x, x).lower () for x in d [k]
                                  if x != ' '
                                 )
                        )
                np.append (str (usercount))
                usercount += 1
                d ['BENUTZERNAME'] = '.'.join (np)
            if d ['PASSWORT'] :
                d ['PASSWORT'] = randpw (len (d ['PASSWORT']))
            if d ['BPK'] :
                d ['BPK'] = base64.b64encode (randstr (20)).decode ('ascii')
            if d ['GEBURTSDATUM'] :
                d1, d2 = d ['GEBURTSDATUM'].split (' ')
                y, m, day = d1.split ('-')
                m = '%02d' % rand.randint (1, 12)
                day = '%02d' % rand.randint (1, 28)
                d ['GEBURTSDATUM'] = ('%s-%s-%s %s' % (y, m, day, d2))
            for k in randnums :
                if d [k] :
                    v = str (next (randnums [k][0]))
                    if randnums [k][1] :
                        v = v + '.0'
                        if d [k].startswith ('-') :
                            v = '-' + v
                        d [k] = v
            for k in hexnums :
                if d [k] :
                    d [k] = hexlify (randstr (hexnums [k])).decode ('ascii')
            wr.writerow (d)


