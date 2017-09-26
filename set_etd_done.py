#!/usr/bin/python3

import sys
import os
from ldap3         import Server, Connection, SCHEMA, BASE, LEVEL
from ldap3         import ALL_ATTRIBUTES, DEREF_NEVER, SUBTREE
from ldap3         import MODIFY_REPLACE, MODIFY_DELETE, MODIFY_ADD
from argparse      import ArgumentParser
from ldaptimestamp import LdapTimeStamp

class LDAP_Access (object) :

    def __init__ (self, args) :
        self.args   = args
        self.srv    = Server (self.args.uri, get_info = SCHEMA)
        self.ldcon  = Connection \
            (self.srv, self.args.bind_dn, self.args.password)
        self.ldcon.bind ()
        assert self.ldcon.bound
    # end def __init__

    def set_etd_done (self, base_dn) :
        """ Get entries marked deleted and set etdTimestamp to
            etlTimestamp
        """
        if self.args.uniqueid :
            r = self.ldcon.search \
                ( base_dn, '(phonlineUniqueId=%s)' % self.args.uniqueid
                , search_scope = SUBTREE
                , attributes   = ALL_ATTRIBUTES
                )
        else :
            r = self.ldcon.search \
                ( base_dn, '(idnDeleted=*)'
                , search_scope = SUBTREE
                , attributes   = ALL_ATTRIBUTES
                )
        if r :
            for ldrec in self.ldcon.response [:] :
                if 'ph15' in ldrec ['dn'] :
                    continue
                attr = ldrec ['attributes']
                if 'etlTimestamp' not in attr :
                    print ("No etlTimestamp: %s" % ldrec ['dn'])
                    continue
                ts  = attr ['etlTimestamp']
                lts = LdapTimeStamp (ts).as_generalized_time ()
                if attr.get ('etdTimestamp') == ts :
                    continue
                if 'etdTimestamp' in attr :
                    changes = dict (etdTimestamp = (MODIFY_REPLACE, [lts]))
                else :
                    changes = dict (etdTimestamp = (MODIFY_ADD, [lts]))
                r = self.ldcon.modify (ldrec ['dn'], changes)
                if not r :
                    msg = \
                        ( "Error on LDAP modify: "
                          "%(description)s: %(message)s"
                          " (code: %(result)s)"
                        % self.ldap.result
                        )
                    print (msg, file=sys.stderr)
    # end def set_etd_done

# end class LDAP_Access

def main () :
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( "-P", "--password"
        , help    = "Password(s) for binding to LDAP"
        , default = 'changeit'
        )
    default_ldap = os.environ.get ('LDAP_URI', 'ldap://06openldap:8389')
    cmd.add_argument \
        ( '-u', '--uri'
        , help    = "LDAP uri, default=%(default)s"
        , default = default_ldap
        )
    cmd.add_argument \
        ( '-U', '--uniqueid'
        , help    = "pk_uniquid for record to mark"
        )
    cmd.add_argument \
        ( "-B", "--bind-dn"
        , dest    = "bind_dn"
        , help    = "Bind-DN, default=%(default)s"
        , default = "cn=admin,o=BMUKK"
        )
    args = cmd.parse_args ()
    ld = LDAP_Access (args)
    ld.set_etd_done ('o=BMUKK')
# end def main

if __name__ == '__main__' :
    main ()
