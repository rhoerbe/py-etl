#!/usr/bin/python3

from ldap3     import Server, Connection, ALL, SCHEMA, BASE, LEVEL, DEREF_NEVER
from ldap3     import ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES
from argparse  import ArgumentParser
from getpass   import getpass
import sys
import os
"""Matthias Kasper Diesen Kommentar löschen."""
class LDAP_Access (object) :

    attributes = \
        ( 'cn'
        , 'nspmDistributionPassword'
        , 'givenName'
        , 'phonlineAccStBediensteter'
        , 'phonlineAccStStudent'
        , 'phonlineAccStWeiterbildung'
        , 'phonlineBediensteterAktiv'
        , 'phonlineBenutzergruppe'
        , 'phonlineBPK'
        , 'phonlineChipIDBediensteter'
        , 'phonlineChipIDStudent'
        , 'phonlineChipIDWeiterbildung'
        , 'phonlineEmailBediensteter'
        , 'phonlineEmailStudent'
        , 'phonlineFunktionen'
        , 'phonlineGebDatum'
        , 'phonlineIdentNr'
        , 'phonlineMatrikelnummer'
        , 'phonlineMirfareIDBediensteter'
        , 'phonlineMirfareIDStudent'
        , 'phonlineMirfareIDWeiterbildung'
        , 'phonlineOrgEinheiten'
        , 'phonlinePersonNr'
        , 'phonlinePersonNrOBF'
        , 'phonlinePersonNrOBFStudent'
        , 'phonlinePersonNrStudent'
        , 'phonlineSapPersnr'
        , 'phonlineSchulkennzahl'
        , 'phonlineSchulkennzahlen'
        , 'phonlineStudentAktiv'
        , 'phonlineUniqueId'
        , 'phonlineWeiterbildungAktiv'
        , 'sn'
        , 'uid'
        )

    def __init__ (self, args, second = False) :
        self.args   = args
        if '%s' in self.args.bind_dn :
            self.binddn = self.args.bind_dn % args.username
        else :
            self.binddn = self.args.bind_dn
        self.basedn = self.args.base_dn
        uri = self.args.uri
        pw  = self.args.password
        if second :
            self.binddn = self.args.bind_dn2
            self.basedn = self.args.base_dn2
            uri = self.args.uri2
            pw = self.args.password2
        self.dn_set = self.get_dn_set
        if self.args.paged_search :
            self.dn_set = self.get_dn_set_paged_search
        self.verbose (self.args.uri, self.binddn)
        self.srv    = Server (uri, get_info = ALL)
        self.ldcon  = Connection (self.srv, self.binddn, pw)
        self.ldcon.bind ()
        self.verbose ("Bound: %s" % self.ldcon.bound)
        self.verbose ("LDCON:", self.ldcon)
        self.verbose ("Server-info:", self.srv.info)
        if self.args.action == 'schema' :
            print (self.srv.schema)
            sys.exit (0)
    # end def __init__

    def get_dn_set_paged_search (self, basedn) :
            filt = '(objectclass=*)'
            return set \
                ( x ['dn'] for x in self.ldcon.extend.standard.paged_search
                    ( basedn, filt
                    , search_scope        = LEVEL
                    , dereference_aliases = DEREF_NEVER
                    , paged_size          = 500
                    , generator           = False
                    )
                )
    # end def get_dn_set_paged_search

    def get_dn_set (self, basedn) :
            filt = '(objectclass=*)'
            result = self.ldcon.search (basedn, filt, search_scope = LEVEL)
            if not result :
                return set ()
            return set (x ['dn'] for x in self.ldcon.response)
    # end def get_dn_set_paged_search

    def get_item (self, dn) :
        """ Get single item by dn """
        filt = '(objectclass=*)'
        #if dn.startswith ('cn=') :
        #    filt = '(%s)' % (dn.split (',', 1) [0])
        #    print (filt)
        # Get attributes of current basedn
        r = self.ldcon.search \
            ( dn, filt
            , search_scope        = BASE
            , dereference_aliases = DEREF_NEVER
            , attributes          = ALL_ATTRIBUTES
            #, attributes          = self.attributes
            )
        if not r :
            return None
        assert len (self.ldcon.response) == 1
        return self.ldcon.response [0]
    # end def get_item

    def iter (self, basedn = None) :
        if basedn is None :
            basedn = self.basedn
        r = self.get_item (basedn)
        if not r :
            print (basedn)
        assert r
        yield r
        # FIXME: Workaround for bug in OpenLDAP: if this is a
        # leaf-node, it will search the whole database sequentially
        # for sub-nodes here. So since we know the structure we
        # don't recurse here.
        # Removed this workaround, seems to occur only with paged
        # search which is no longer the default.
        if True or len (r ['dn'].split (',')) < 4 :
            dns = self.dn_set (basedn)
            for dn in sorted (dns, key = lambda x : x.lower ()) :
                if dn == basedn :
                    continue
                for i in self.iter (dn) :
                    yield i
    # end def iter

    def short_dn (self, dn) :
        assert dn.endswith (self.basedn)
        dn = dn [:len (dn) - len (self.basedn)].lower ()
        return dn
    # end def short_dn

    def verbose (self, *msg) :
        if self.args.verbose :
            print (*msg)
    # end def verbose

    def __getattr__ (self, name) :
        """ Delegate to our ldcon, caching variant """
        if name.startswith ('_') :
            raise AttributeError (name)
        r = getattr (self.ldcon, name)
        setattr (self, name, r)
        return r
    # end def __getattr__

# end class LDAP_Access

def multival_fixup (oldval) :
    return ';'.join (sorted (oldval.split (';')))
# end def multival_fixup

compare_fixup = dict \
    ( phonlineBenutzergruppe = multival_fixup
    )

compare_ignore = set \
    (( 'objectClass'
     , 'ACL'
     , 'DirXML-Associations'
     , 'passwordUniqueRequired'
     , 'DirXML-PasswordSyncStatus'
     , 'DirXML-ADContext'
     , 'passwordMinimumLength'
     , 'DirXML-ADAliasName'
     , 'nspmDistributionPassword'
     , 'passwordAllowChange'
     , 'passwordRequired'
     , 'etdTimestamp'
     , 'etlTimestamp'
     , 'idnDistributionPassword'
     , 'userPassword'
     , 'patchlevel'
    ))

iter_ignore = set \
    (( 'patchlevel'
     , 'etlTimestamp'
     , 'etdTimestamp'
    ))

def main () :
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( 'action'
        , help    = 'Action to perform, one of "schema", "compare", '
                    '"iter", "getdn", note that for getdn the specified '
                    'base_dn is fetched'
        )
    cmd.add_argument \
        ( "-2", "--second"
        , help    = "Use second instance for actions other than compare"
        , action  = "store_true"
        )
    cmd.add_argument \
        ( "-B", "--bind-dn"
        , dest    = "bind_dn"
        , help    = "Bind-DN, default=%(default)s, "
                    "Username is put into bind-dn at %%s location"
        , default = "cn=%s,ou=ssvc,o=phoinfra"
        )
    cmd.add_argument \
        ( "--bind-dn2"
        , dest    = "bind_dn2"
        , help    = "Bind-DN 2, default=%(default)s"
        , default = "cn=admin,o=BMUKK"
        )
    cmd.add_argument \
        ( "-d", "--base-dn"
        , dest    = "base_dn"
        , help    = "Base-DN for starting search, default=%(default)s"
        , default = 'ou=phq08,o=BMUKK-QS'
        )
    cmd.add_argument \
        ( "--base-dn2"
        , dest    = "base_dn2"
        , help    = "Base-DN 2 for starting search, default=%(default)s"
        , default = 'ou=ph08,o=BMUKK'
        )
    cmd.add_argument \
        ( "-P", "--password"
        , dest    = "password"
        , help    = "Password for binding"
        )
    cmd.add_argument \
        ( "-p", "--paged-search"
        , help    = "Use paged search for getting DNs"
        , action  = "store_true"
        , default = False
        )
    cmd.add_argument \
        ( "--password2"
        , dest    = "password2"
        , help    = "Password for binding to 2nd instance"
        , default = "changeit"
        )
    cmd.add_argument \
        ( "-U", "--username"
        , dest    = "username"
        , help    = "Username in bind-dn"
        , default = "rschlatterbeck"
        )
    cmd.add_argument \
        ( "-u", "--uri"
        , dest    = "uri"
        , help    = "LDAP uri, default=%(default)s"
        , default = 'ldaps://172.18.81.8:636'
        )
    default_ldap = os.environ.get ('LDAP_URI', 'ldap://06openldap:8389')
    cmd.add_argument \
        ( "--uri2"
        , dest    = "uri2"
        , help    = "LDAP uri, default=%(default)s"
        , default = default_ldap
        )
    cmd.add_argument \
        ( "-v", "--verbose"
        , help    = "Verbose messages"
        , action  = "store_true"
        )
    args = cmd.parse_args ()
    if not args.password  and (args.action == 'compare' or not args.second) :
        args.password  = getpass ("1st Bind Password: ")
    if not args.password2 and (args.action == 'compare' or args.second) :
        args.password2 = getpass ("2nd Bind Password: ")
    ld = LDAP_Access (args, second = args.second)
    #print (args.second)
    assert ld.bound
    if args.action == 'getdn' :
        print (ld.get_item (args.base_dn))
    if args.action == 'iter' :
        count = 0
        for x in ld.iter () :
            print (x ['dn'], end = ' ')
            for k in sorted (x ['attributes'].keys ()) :
                if k in iter_ignore :
                    continue
                v = x ['attributes'][k]
                if k == 'userPassword' :
                    assert len (v) == 1
                    v = v [0].decode ('ascii').split ('}', 1) [0] + '}'
                print ("%s=%s" % (k, v), end = ' ')
            print ("")
            count += 1
        print ("\n\nCount:", count)
    if args.action == 'compare' :
        ld2 = LDAP_Access (args, second = True)
        assert ld2.bound
        count = 0
        i1 = ld.iter  ()
        i2 = ld2.iter ()
        x1 = next (i1)
        x2 = next (i2)
        while (True) :
            dn1 = ld.short_dn  (x1 ['dn'])
            dn2 = ld2.short_dn (x2 ['dn'])

            while dn1 != dn2 :
                #print ("DNs: %s %s" % (dn1, dn2))
                while dn1 < dn2 :
                    print ("Only in lhs: %s" % x1 ['dn'])
                    x1  = next (i1)
                    dn1 = ld.short_dn  (x1 ['dn'])
                while dn2 < dn1 :
                    print ("Only in rhs: %s" % x2 ['dn'])
                    x2  = next (i2)
                    dn2 = ld2.short_dn (x2 ['dn'])
            x1a = set (x1 ['attributes'].keys ()) - compare_ignore
            x2a = set (x2 ['attributes'].keys ()) - compare_ignore
            if x1a - x2a :
                print \
                    ( "Attributes of %s only in lhs: %s"
                    % (x1 ['dn'], sorted (list (x1a - x2a)))
                    )
            if x2a - x1a :
                print \
                    ( "Attributes of %s only in rhs: %s"
                    % (x2 ['dn'], sorted (list (x2a - x1a)))
                    )
            for a in sorted (x1a & x2a) :
                v1 = x1 ['attributes'][a]
                v2 = x2 ['attributes'][a]
                if a in compare_fixup :
                    fixup = compare_fixup [a]
                    v1 = fixup (v1)
                    v2 = fixup (v2)
                if v1 != v2 :
                    print \
                        ( "Differs: %s %s: (%s vs %s)"
                        % (x1 ['dn'], a, repr (v1), repr (v2))
                        )
            #print ("%s\r" % count, end = '')
            sys.stdout.flush ()
            count += 1
            x1 = next (i1)
            x2 = next (i2)
# end def main

if __name__ == '__main__' :
    main ()
