#!/usr/bin/python3

import os
import sys
import time

from argparse         import ArgumentParser
from ldap3            import Server, Connection, SCHEMA, BASE, LEVEL
from ldap3            import ALL_ATTRIBUTES, DEREF_NEVER, SUBTREE
from ldap3            import MODIFY_REPLACE, MODIFY_DELETE, MODIFY_ADD
from traceback        import format_exc

def log_debug (msg) :
    print (msg, file = sys.stderr)
    sys.stderr.flush ()
# end def log_debug

def log_info (msg) :
    print ("Info:", msg, file = sys.stderr)
    sys.stderr.flush ()
# end def log_info

def log_warn (msg) :
    print ("Warning:", msg, file = sys.stderr)
    sys.stderr.flush ()
# end def log_warn

def log_error (msg) :
    print ("Error:", msg, file = sys.stderr)
    sys.stderr.flush ()
# end def log_error

class Namespace (dict) :
    def __getattr__ (self, key) :
        try :
            return self [key]
        except KeyError as ke :
            raise AttributeError (ke)
    # end def __getattr__
# end class Namespace

class ApplicationError (Exception) :
    pass

class LDAP_Access (object) :

    attributes = \
        ('phonlineStudentAktiv', 'phonlineEmailStudent', 'phonlineAccStStudent')

    def __init__ (self, args) :
        self.args  = args
        # FIXME: Poor-mans logger for now
        self.log = Namespace ()
        self.log ['debug'] = log_debug
        self.log ['error'] = log_error
        self.log ['warn']  = log_warn
        self.log ['info']  = log_info

        for dn in self.args.base_dn :
            if 'ph15' in dn :
                self.ph15dn = dn
                break
        else :
            raise ApplicationError ("No ph15 dn specified, nothing to do")

        self.srv    = Server (self.args.uri, get_info = SCHEMA)
        self.ldcon  = Connection \
            (self.srv, self.args.bind_dn, self.args.password)
        self.bind_ldap ()
    # end def __init__

    def dn15 (self, dn) :
        offset = dn.index ('ou=ph')
        assert (offset > 0)
        return dn [0:offset] + 'ou=ph15' + dn [offset+7:]
    # end def dn15

    def bind_ldap (self) :
        while not self.ldcon.bound :
            self.ldcon.bind ()
            if not self.ldcon.bound :
                msg = \
                    ( "Error on LDAP bind: %(description)s: %(message)s"
                      " (code: %(result)s)"
                    % self.ldcon.result
                    )
                self.log.error (msg)
                if self.args.terminate :
                    raise RuntimeError (msg)
                time.sleep (5)
    # end def bind_ldap

    def sync_stud_emails (self) :
        for dn in self.args.base_dn :
            if 'ph15' in dn :
                continue
            self.log.info ("BASE-DN: %s" % dn)
            r = self.ldcon.search \
                ( dn, '(&(phonlineEmailStudent=*)(phonlineStudentAktiv=J))'
                , search_scope = SUBTREE
                , attributes   = self.attributes
                )
            if not r :
                self.log.warn ("No phonlineEmailStudent in %s" % dn)
                continue
            for ldrec in self.ldcon.response [:] :
                attr = ldrec ['attributes']
                d15  = self.dn15 (ldrec ['dn'])
                if not attr.get ('phonlineEmailStudent') :
                    self.log.warn \
                        ("DN %s no phonlineEmailStudent" % ldrec ['dn'])
                    continue
                mail = attr ['phonlineEmailStudent']
                r = self.ldcon.search \
                    ( d15, '(objectClass=*)'
                    , search_scope = BASE
                    , attributes   = ('phonlineEmailStudent')
                    )
                if not r :
                    self.log.warn ("DN %s not found" % d15)
                    continue
                assert len (self.ldcon.response) == 1

                attr15 = self.ldcon.response [0]['attributes']
                # Nothing to do if same address
                mail15 = attr15.get ('phonlineEmailStudent')
                if mail15 == mail :
                    continue
                if mail15 :
                    self.log.warn \
                        ( "DN %s changes phonlineEmailStudent in ph15 %s -> %s"
                        % (ldrec ['dn'], mail15, mail)
                        )
                mod = MODIFY_ADD
                if mail15 :
                    mod = MODIFY_REPLACE
                changes = dict (phonlineEmailStudent = (mod, [mail]))
                r = self.ldcon.modify (d15, changes)
                if not r :
                    msg = \
                        ( "Error on LDAP modify: "
                          "%(description)s: %(message)s"
                          " (code: %(result)s)"
                        % self.result
                        )
                    self.log.error (msg + "DN=%s" % d15)
        self.log.info ("SUCCESS")
        if not self.args.terminate :
            while True :
                time.sleep (60)
    # end def sync_stud_emails
# end class LDAP_Access

def main () :
    cmd = ArgumentParser ()
    default_bind_dn = os.environ.get ('LDAP_BIND_DN', 'cn=admin,o=BMUKK')
    cmd.add_argument \
        ( "-B", "--bind-dn"
        , help    = "Bind-DN, default=%(default)s"
        , default = default_bind_dn
        )
    cmd.add_argument \
        ( "-d", "--base-dn"
        , help    = "Base-DN for starting search, usually configured via "
                    "environment, will use *all* databases specified, "
                    "can be specified more than once. Note that ph15 must "
                    "be specified, otherwise nothing will be done."
        , action  = 'append'
        , default = []
        )
    # Get default_pw from /etc/conf/passwords LDAP_PASSWORD entry.
    # Also get password-encryption password when we're at it
    ldap_pw = 'changeme'
    try :
        with open ('/etc/conf/passwords', 'r') as f :
            for line in f :
                if line.startswith ('LDAP_PASSWORD') :
                    ldap_pw = line.split ('=', 1) [-1].strip ()
    except FileNotFoundError :
        pass
    cmd.add_argument \
        ( "-P", "--password"
        , help    = "Password(s) for binding to LDAP"
        , default = ldap_pw
        )
    cmd.add_argument \
        ( '-t', '--terminate'
        , help    = "Terminate container after initial_load"
        , action  = "store_true"
        , default = False
        )
    default_ldap = os.environ.get ('LDAP_URI', 'ldap://06openldap:8389')
    cmd.add_argument \
        ( '-u', '--uri'
        , help    = "LDAP uri, default=%(default)s"
        , default = default_ldap
        )
    args = cmd.parse_args ()
    if not args.base_dn :
        args.base_dn   = []
        for inst in os.environ ['DATABASE_INSTANCES'].split (',') :
            db, dummy = (x.strip () for x in inst.split (':'))
            dn = ','.join \
                (( os.environ ['LDAP_USER_OU']
                ,  'ou=%s' % db
                ,  os.environ ['LDAP_BASE_DN']
                ))
            args.base_dn.append   (dn)

    try :
        ldap = LDAP_Access (args)
        ldap.sync_stud_emails ()
    except ApplicationError as cause :
        log_error (str (cause))
        if not args.terminate :
            while True :
                time.sleep (60)
    except Exception :
        log_error (format_exc ())
        if not args.terminate :
            while True :
                time.sleep (60)
# end def main

if __name__ == '__main__' :
    main ()
