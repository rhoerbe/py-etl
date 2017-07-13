#!/usr/bin/python3

from ldap3     import Server, Connection, ALL, SCHEMA, BASE, LEVEL, DEREF_NEVER
from ldap3     import ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES
from argparse  import ArgumentParser
from getpass   import getpass
import sys
import os

class LDAP_Access (object) :

    def __init__ (self, args, second = False) :
        self.args   = args
        if '%s' in self.args.bind_dn :
            self.binddn = self.args.bind_dn % args.username
        else :
            self.binddn = self.args.bind_dn
        uri = self.args.uri
        pw  = self.args.password
        if second :
            self.binddn = self.args.bind_dn2
            uri = self.args.uri2
            pw = self.args.password2
        self.verbose (self.args.uri)
        self.srv    = Server (uri, get_info = ALL)
        self.ldcon  = Connection (self.srv, self.binddn, pw)
        self.ldcon.bind ()
        self.verbose ("Bound: %s" % self.ldcon.bound)
        self.verbose (self.ldcon)
        self.verbose (self.srv.info)
        if self.args.action == 'schema' :
            print (self.srv.schema)
            sys.exit (0)
    # end def __init__

    def get_item (self, dn) :
        """ Get single item by dn """
        filt = '(objectclass=*)'
        # Get attributes of current basedn
        r = self.ldcon.search \
            ( dn, filt
            , search_scope        = BASE
            , dereference_aliases = DEREF_NEVER
            , attributes          = ALL_ATTRIBUTES
            )
        if not r :
            return None
        assert len (self.ldcon.response) == 1
        return self.ldcon.response [0]
    # end def get_item

    def iter (self, basedn = None) :
        if basedn is None :
            basedn = self.args.base_dn
        r = self.get_item (basedn)
        assert r
        yield r
        filt = '(objectclass=*)'
        for entry in sorted \
            ( self.ldcon.extend.standard.paged_search
                ( basedn, filt
                , search_scope        = LEVEL
                , dereference_aliases = DEREF_NEVER
                , paged_size          = 500
                )
            , key = lambda x : x ['dn']
            ) :
            if entry ['dn'] == basedn :
                continue
            for i in self.iter (entry ['dn']) :
                yield i
    # end def iter

    def verbose (self, msg) :
        if self.args.verbose :
            print (msg)
    # end def verbose

# end class LDAP_Access

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
        , default = "cn=%s,ou=auth,o=BMUKK"
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
        , default = 'ou=ph08,o=BMUKK'
        )
    cmd.add_argument \
        ( "-P", "--password"
        , dest    = "password"
        , help    = "Password for binding"
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
        )
    cmd.add_argument \
        ( "-u", "--uri"
        , dest    = "uri"
        , help    = "LDAP uri, default=%(default)s"
        , default = 'ldaps://172.18.81.8:636'
        )
    # Compute the default ldap server from environment of container
    # In production we'll have an argument here
    default_ldap = os.environ.get ('8389_PORT').replace ('tcp', 'ldap')
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
    if not args.password and (args.action == 'compare' or not args.second) :
        args.password = getpass ("1st Bind Password: ")
    if not args.password and (args.action == 'compare' or args.second) :
        args.password = getpass ("2nd Bind Password: ")
    ld = LDAP_Access (args, second = args.second)
    if args.action == 'getdn' :
        print (ld.get_item (args.base_dn))
    if args.action == 'iter' :
        count = 0
        for x in ld.iter () :
            if x ['dn'].endswith (',ou=user,ou=ph08,o=BMUKK') :
                if count == 5 :
                    print (x)
                count += 1
                print (x ['dn'], count, "\r", end = '')
            else :
                print (x)
        print ("\n\nCount:", count)
    if args.action == 'compare' :
        ld2 = LDAP_Access (args, second = True)
        count = 0
        i1 = ld.iter  ()
        i2 = ld2.iter ()
        for x1, x2 in zip (i1, i2) :
            dn1 = x1 ['dn']
            dn2 = x2 ['dn']
            while dn1 != dn2 :
                while i1 ['dn'] < i2 ['dn'] :
                    print ("Only in lhs: %s" % dn1)
                    x1 = i1.next ()
                while i2 ['dn'] < i1 ['dn'] :
                    print ("Only in rhs: %s" % dn2)
                    x2 = i2.next ()
                x1a = set (x1 ['attributes'].keys ()) - compare_ignore
                x2a = set (x2 ['attributes'].keys ()) - compare_ignore
                if x1a - x2a :
                    print \
                        ( "Attributes of %s only in lhs: %s"
                        % (dn1, sorted (list (x1a - x2a)))
                        )
                if x2a - x1a :
                    print \
                        ( "Attributes of %s only in rhs: %s"
                        % (dn2, sorted (list (x2a - x1a)))
                        )
                for a in sorted (x1a & x2a) :
                    v1 = x1 ['attributes'][a]
                    v2 = x2 ['attributes'][a]
                    if v1 != v2 :
                        print ("Differs: %s (%r vs %r)" % (dn1, v1, v2))
            print ("%s\r" % count, end = '')
            sys.stdout.flush ()
            count += 1
# end def main

if __name__ == '__main__' :
    main ()
