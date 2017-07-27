#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import base64
from   csv      import DictReader, DictWriter
from   random   import SystemRandom
from   binascii import hexlify

class Randnum (object) :
    """ Returns a number from the given range
    """
    def __init__ (self, start, end) :
        self.start     = start
        self.end       = end
        self.rand      = SystemRandom ()
    # end def __init__

    def __iter__ (self) :
        while True :
            yield self.rand.randint (self.start, self.end)
    # end def __iter__

# end class Randnum

class Anonymizer (object) :

    text_hi = "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ "
    text_lo = "abcdefghijklmnopqrstuvwxyzäöüß "
    pwchr   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567899"
    textmap = { 'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 's'
              , 'Ä': 'A', 'Ö': 'O', 'Ü': 'U'
              }

    #                              random range                float
    randnums = dict \
        ( pm_sap_personalnummer = (iter (Randnum (1111111, 99999999)),  False)
        , person_nr             = (iter (Randnum (11111, 999999)),      True)
        , st_person_nr          = (iter (Randnum (11111, 999999)),      True)
        , ident_nr              = (iter (Randnum (11111, 999999)),      True)
        , matrikelnummer        = (iter (Randnum (11111111, 99999999)), False)
        , chipid_a              =
            (iter (Randnum (1111111111111111, 9999999999999999)), False)
        , chipid_b              =
            (iter (Randnum (1111111111111111, 9999999999999999)), False)
        , chipid_st             =
            (iter (Randnum (1111111111111111, 9999999999999999)), False)
        )

    hexnums = dict \
        ( person_nr_obf    = 8
        , st_person_nr_obf = 8
        , mirfareid_b      = 4
        , mirfareid_st     = 4
        , mirfareid_a      = 4
        )

    def __init__ (self) :
        self.rand      = SystemRandom ()
        self.randint   = self.rand.randint
        self.usercount = 0
        self.ui_count  = 4711
        # Remember values during run and re-insert them for the same
        # unique-id if no other value is in the dataset
        self.values    = {}
    # end def __init__

    def randname (self, l) :
        result = []
        hilen  = len (self.text_hi)
        lolen  = len (self.text_lo)
        result.append (self.text_hi [self.randint (0, hilen - 1)])
        for k in range (l - 1) :
            result.append (self.text_lo [self.randint (0, lolen - 1)])
        return ''.join (result)
    # end def randname

    def randascii (self, l) :
        return ''.join (self.textmap.get (x, x) for x in self.randname (l))
    # end def randascii

    def randstr (self, l) :
        return os.urandom (l)
    # end def randstr

    def randpw (self, l) :
        result = []
        for k in range (l) :
            result.append (self.pwchr [self.randint (0, len (self.pwchr) - 1)])
        return ''.join (result)
    # end def randpw

    def set (self, d, k, v) :
        ui = self.ui
        if d [k] :
            if k in self.values [ui] and self.values [ui][k][0] == d [k] :
                d [k] = self.values [ui][k][1]
            else :
                self.values [ui][k] = [d [k]]
                d [k] = v
                self.values [ui][k].append (d [k])
    # end def set

    def _anonymize (self, d, wr) :
        self.ui = ui = d ['pk_uniqueid']
        if ui not in self.values :
            self.values [ui] = {}
        for k in 'vorname', 'nachname' :
            self.set (d, k, self.randname (len (d [k])))
        for k in 'emailadresse_b', 'emailadresse_st' :
            if d [k] :
                if '@' in d [k] :
                    email, domain = d [k].split ('@')
                else :
                    email = d [k]
                    domain = 'example.com'
                v = self.randascii (len (email)) + '@' + domain
                self.set (d, k, v)
        k = 'benutzername'
        if d [k] :
            np = []
            for k2 in 'vorname', 'nachname' :
                np.append \
                    (''.join (self.textmap.get (x, x).lower ()
                              for x in d [k2]
                              if x != ' '
                             )
                    )
            np.append (str (self.usercount))
            self.usercount += 1
            self.set (d, k, '.'.join (np))
        self.set (d, 'passwort', self.randpw (len (d ['passwort'])))
        self.set \
            (d, 'bpk', base64.b64encode (self.randstr (20)).decode ('ascii'))
        k = 'geburtsdatum'
        if d [k] :
            d1, d2 = d [k].split (' ')
            y, m, day = d1.split ('-')
            m = '%02d' % self.randint (1, 12)
            day = '%02d' % self.randint (1, 28)
            self.set (d, k, '%s-%s-%s %s' % (y, m, day, d2))
        self.set (d, 'pk_uniqueid', ('%d' % self.ui_count) + '.0')
        self.ui_count += 1
        for k in self.randnums :
            if d [k] :
                v = str (next (self.randnums [k][0]))
                if self.randnums [k][1] :
                    v = v + '.0'
                if d [k].startswith ('-') :
                    v = '-' + v
                self.set (d, k, v)
        for k in self.hexnums :
            self.set \
                ( d, k
                , hexlify (self.randstr (self.hexnums [k])).decode ('ascii')
                )
        wr.writerow (d)
    # end def _anonymize

    def anonymize (self, ifn) :
        ofn = ifn + '.anonymized'
        with open (ifn, 'r', encoding = 'utf-8') as ifile :
            dr = DictReader (ifile, delimiter = ';')
            wr = None
            with open (ofn, 'w', encoding = 'utf-8') as ofile :
                for d in dr :
                    if wr is None :
                        wr = DictWriter (ofile, dr.fieldnames, delimiter = ';')
                        wr.writeheader ()
                    self._anonymize (d, wr)
    # end def anonymize
# end class Anonymizer


if __name__ == '__main__' :
    a = Anonymizer ()
    for ifn in sys.argv [1:] :
        a.anonymize (ifn)
