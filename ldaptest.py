#!/usr/bin/python3

from ldap3    import Server, Connection, ALL, SCHEMA, BASE, LEVEL, DEREF_NEVER
from ldap3    import ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES
from argparse import ArgumentParser
from getpass  import getpass
import sys

class LDAP_Access (object) :

    def __init__ (self, args) :
        self.args   = args
        if '%s' in self.args.bind_dn :
            self.binddn = self.args.bind_dn % args.username
        else :
            self.binddn = self.args.bind_dn
        self.verbose (self.args.uri)
        self.srv    = Server (self.args.uri, get_info = ALL)
        self.ldcon  = Connection (self.srv, self.binddn, self.args.password)
        self.ldcon.bind ()
        self.verbose ("Bound: %s" % self.ldcon.bound)
        self.verbose (self.ldcon)
        self.verbose (self.srv.info)
        #print (self.srv.schema)
        #sys.exit (23)
    # end def __init__

    def iter (self, basedn = None) :
        if basedn is None :
            basedn = self.args.base_dn
        filt = '(objectclass=*)'
        # Get attributes of current basedn
        r = self.ldcon.search \
            ( basedn, filt
            , search_scope        = BASE
            , dereference_aliases = DEREF_NEVER
            , attributes          = ALL_ATTRIBUTES
            )
        assert r
        assert len (self.ldcon.response) == 1
        yield self.ldcon.response [0]
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

def main () :
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( "-B", "--bind-dn"
        , dest    = "bind_dn"
        , help    = "Bind-DN, default=%(default)s, "
                    "Username is put into bind-dn at %%s location"
        , default = "cn=%s,ou=auth,o=BMUKK"
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
    cmd.add_argument \
        ( "-v", "--verbose"
        , help    = "Verbose messages"
        , action  = "store_true"
        )
    args = cmd.parse_args ()
    if not args.password :
        args.password = getpass ("Bind Password: ")
    ld = LDAP_Access (args)
    count = 0
    for x in ld.iter () :
        if x ['dn'].endswith (',ou=user,ou=ph08,o=BMUKK') :
            if not count :
                print (x)
            count += 1
        else :
            print (x)
    print ("Count:", count)
# end def main

if __name__ == '__main__' :
    main ()
