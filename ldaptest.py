#!/usr/bin/python3

from ldap3    import Server, Connection, ALL, SCHEMA, BASE, LEVEL, DEREF_NEVER
from ldap3    import ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES
from argparse import ArgumentParser
from getpass  import getpass

class LDAP_Access (object) :

    def __init__ (self, args) :
        self.args   = args
        self.binddn = "cn=%s,ou=auth,o=BMUKK" % args.username
        self.srv    = Server (self.args.uri, get_info = SCHEMA)
        self.ldcon  = Connection (self.srv, self.binddn, self.args.password)
        self.ldcon.bind ()
        #print ("Bound: %s" % self.ldcon.bound)
        #print (self.ldcon)
        #print (self.srv.info)
        #print (self.srv.schema)
    # end def __init__

    def iter (self, basedn = 'ou=ph08,o=BMUKK') :
        filt = '(objectclass=*)'
        # Get attributes of current basedn
        r = self.ldcon.search \
            ( basedn, filt
            , search_scope        = BASE
            , dereference_aliases = DEREF_NEVER
            , attributes          = ALL_OPERATIONAL_ATTRIBUTES
            )
        assert r
        assert len (self.ldcon.response) == 1
        yield self.ldcon.response [0]
        r = self.ldcon.search \
            ( basedn, filt
            , search_scope        = LEVEL
            , dereference_aliases = DEREF_NEVER
            )
        if not r :
            #print (self.ldcon.last_error)
            raise StopIteration ()
        for s in sorted (self.ldcon.response, key = lambda x : x ['dn']) :
            for i in self.iter (s ['dn']) :
                yield i
    # end def iter

# end class LDAP_Access

def main () :
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( "-u", "--uri"
        , dest    = "uri"
        , help    = "LDAP uri, default=%(default)s"
        , default = 'ldaps://172.18.81.8:636'
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
    args = cmd.parse_args ()
    if not args.password :
        args.password = getpass ("Bind Password: ")
    ld = LDAP_Access (args)
    count = 0
    for x in ld.iter () :
        if x ['dn'].endswith (',ou=user,ou=ph08,o=BMUKK') :
            count += 1
        else :
            print (x)
    print ("Count:", count)
# end def main

if __name__ == '__main__' :
    main ()
