#!/usr/bin/python3

from ldap3    import Server, Connection, ALL
from argparse import ArgumentParser
from getpass  import getpass

class LDAP_Access (object) :

    def __init__ (self, args) :
        self.args   = args
        self.binddn = "cn=%s,ou=auth,o=BMUKK" % args.username
        self.srv    = Server (self.args.uri)
        self.ldcon  = Connection (self.srv, self.binddn, self.args.password)
        self.ldcon.bind ()
        print ("Bound: %s" % self.ldcon.bound)
        #print (self.srv.schema)
    # end def __init__

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
# end def main

if __name__ == '__main__' :
    main ()
