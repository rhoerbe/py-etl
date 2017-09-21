#!/usr/bin/python3

import os
import sys
import pyodbc
import pytz
import time

from argparse         import ArgumentParser
from ldap3            import Server, Connection, SCHEMA, BASE, LEVEL
from ldap3            import ALL_ATTRIBUTES, DEREF_NEVER, SUBTREE
from ldap3            import MODIFY_REPLACE, MODIFY_DELETE, MODIFY_ADD
from datetime         import datetime
from ldaptimestamp    import LdapTimeStamp
from aes_pkcs7        import AES_Cipher
from binascii         import hexlify, unhexlify
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

class ApplicationError (Exception) :
    pass

class LDAP_Access (object) :

    def __init__ (self, args, parent) :
        self.args  = args
        # FIXME: Poor-mans logger for now
        self.log = Namespace ()
        self.log ['debug'] = log_debug
        self.log ['error'] = log_error
        self.log ['warn']  = log_warn
        self.log ['info']  = log_info

        self.parent = parent
        self.srv    = Server (self.args.uri, get_info = SCHEMA)
        self.ldcon  = Connection \
            (self.srv, self.args.bind_dn, self.args.password)
        self.bind_ldap ()
    # end def __init__

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

    def get_by_cn (self, cn, base_dn = None) :
        """ Get single item by cn for our basedn or the given dn
        """
        base_dn = base_dn or self.dn
        if not cn.startswith ('cn=') :
            cn = "cn=" + cn
        dn = ','.join ((cn, base_dn))
        return self.get_by_dn (dn)
    # end get_by_cn

    def get_by_dn (self, dn) :
        """ Get entry by dn
        """
        r = self.ldcon.search \
            ( dn, '(objectClass=*)'
            , search_scope = BASE
            , attributes   = ALL_ATTRIBUTES
            )
        if r :
            if len (self.ldcon.response) != 1 :
                self.log.error ("Got more than one record with dn %s" % dn)
            return self.ldcon.response [0]
    # end def get_by_dn

    def get_entries (self, pk_uniqueid, dn = None) :
        """ Get all entries with the same pk_uniqueid.
            Yes: despite the name these are not unique, unfortunately
        """
        dn = dn or self.dn
        r = self.ldcon.search \
            ( dn, '(phonlineUniqueId=%s)' % pk_uniqueid
            , search_scope = LEVEL
            , attributes   = ALL_ATTRIBUTES
            )
        if r :
            if len (self.ldcon.response) != 1 :
                self.log.warn \
                    ( "Got more than one record with pk_uniqueid %s in dn %s"
                    % (pk_uniqueid, dn)
                    )
            # return a copy
            return self.ldcon.response [:]
        return []
    # end def get_entries

    def search_cn_all (self, cn) :
        r = self.ldcon.search \
            ( "o=BMUKK", '(cn=%s)' % cn
            , search_scope = SUBTREE
            , attributes   = ALL_ATTRIBUTES
            )
        if r :
            return self.ldcon.response
        return []
    # end def search_cn_all

    def __getattr__ (self, name) :
        """ Delegate to our ldcon, caching variant """
        if name.startswith ('_') :
            raise AttributeError (name)
        r = getattr (self.ldcon, name)
        # Don't cache!
        return r
    # end def __getattr__

    @property
    def dn (self) :
        return self.parent.dn
    # end def dn

# end class LDAP_Access

class Namespace (dict) :
    def __getattr__ (self, key) :
        try :
            return self [key]
        except KeyError as ke :
            raise AttributeError (ke)
    # end def __getattr__
# end class Namespace

def from_db_date (item) :
    """ Note that phonline stores the only date attribute
        "phonlineGebDatum" as a string!
        Also note: the seconds always contain a trailing '.0' in the
        original LDAP tree.
    """
    if item is None :
        return item
    return item.strftime ("%Y-%m-%d %H:%M:%S") + '.0'
# end def from_db_date

def from_db_number (item) :
    if item is None :
        return item
    return str (int (item))
# end def from_db_number

def from_db_rstrip (item) :
    """ Strip items before writing to LDAP. Note that if the stripping
        results in an empty string we return None (leave attribute empty)
    """
    if item is None :
        return item
    item = item.rstrip ()
    if item :
        return item
    return None
# end def from_db_rstrip

def from_db_strip (item) :
    """ Strip items before writing to LDAP. Note that if the stripping
        results in an empty string we return None (leave attribute empty)
    """
    if item is None :
        return item
    item = item.strip ()
    if item :
        return item
    return None
# end def from_db_strip

def from_multi (item) :
    """ Return array for an item containing several fields separated by
        semicolon in the database
    """
    if item is None :
        return item
    item = item.strip ()
    if not item :
        return None
    return item.split (';')
# end def from_multi

class ODBC_Connector (object) :

    fields = dict \
        ( benutzer_alle_dirxml_v =
            ( 'person_nr_obf'
            , 'st_person_nr_obf'
            , 'org_einheiten'
            , 'emailadresse_b'
            , 'emailadresse_st'
            , 'bpk'
            , 'pm_sap_personalnummer'
            , 'schulkennzahlen'
            , 'funktionen'
            , 'pk_uniqueid'
            , 'vorname'
            , 'nachname'
            , 'benutzername'
            , 'passwort'
            , 'benutzergruppen'
            , 'aktiv_st_person'
            , 'aktiv_a_person'
            , 'aktiv_b_person'
            , 'chipid_b'
            , 'chipid_st'
            , 'chipid_a'
            , 'mirfareid_b'
            , 'mirfareid_st'
            , 'mirfareid_a'
            , 'matrikelnummer'
            , 'account_status_b'
            , 'account_status_st'
            , 'account_status_a'
            , 'geburtsdatum'
            , 'person_nr'
            , 'st_person_nr'
            , 'ident_nr'
            )
        , eventlog_ph =
            ( 'record_id'
            , 'table_key'
            , 'status'
            , 'event_type'
            , 'event_time'
            , 'perpetrator'
            , 'table_name'
            , 'column_name'
            , 'old_value'
            , 'new_value'
            , 'synch_id'
            , 'synch_online_flag'
            , 'transaction_flag'
            , 'read_time'
            , 'error_message'
            , 'attempt'
            , 'admin_notify_flag'
            )
        )
    odbc_to_ldap_field = dict \
        ( account_status_a      = 'phonlineAccStWeiterbildung'
        , account_status_b      = 'phonlineAccStBediensteter'
        , account_status_st     = 'phonlineAccStStudent'
        , aktiv_a_person        = 'phonlineWeiterbildungAktiv'
        , aktiv_b_person        = 'phonlineBediensteterAktiv'
        , aktiv_st_person       = 'phonlineStudentAktiv'
        , benutzergruppen       = 'phonlineBenutzergruppe'
        , benutzername          = 'cn'
        , bpk                   = 'phonlineBPK'
        , chipid_a              = 'phonlineChipIDWeiterbildung'
        , chipid_b              = 'phonlineChipIDBediensteter'
        , chipid_st             = 'phonlineChipIDStudent'
        , emailadresse_b        = 'phonlineEmailBediensteter'
        , emailadresse_st       = 'phonlineEmailStudent'
        , funktionen            = 'phonlineFunktionen'
        , geburtsdatum          = 'phonlineGebDatum'
        , ident_nr              = 'phonlineIdentNr'
        , matrikelnummer        = 'phonlineMatrikelnummer'
        , mirfareid_a           = 'phonlineMirfareIDWeiterbildung'
        , mirfareid_b           = 'phonlineMirfareIDBediensteter'
        , mirfareid_st          = 'phonlineMirfareIDStudent'
        , nachname              = 'sn'
        , org_einheiten         = 'phonlineOrgEinheiten'
        , passwort              = 'idnDistributionPassword'
        , person_nr             = 'phonlinePersonNr'
        , person_nr_obf         = 'phonlinePersonNrOBF'
        , pk_uniqueid           = 'phonlineUniqueId'
        , pm_sap_personalnummer = 'phonlineSapPersnr'
        , schulkennzahlen       = 'phonlineSchulkennzahlen'
        , st_person_nr          = 'phonlinePersonNrStudent'
        , st_person_nr_obf      = 'phonlinePersonNrOBFStudent'
        , vorname               = 'givenName'
        )

    data_conversion = dict \
        ( geburtsdatum          = from_db_date
        , ident_nr              = from_db_number
        , person_nr             = from_db_number
        , st_person_nr          = from_db_number
        , pk_uniqueid           = from_db_number
        , funktionen            = from_multi
        , schulkennzahlen       = from_multi
        , emailadresse_b        = from_db_rstrip
        , emailadresse_st       = from_db_rstrip
        , benutzername          = from_db_strip
        , vorname               = from_db_rstrip
        , nachname              = from_db_rstrip
        , pm_sap_personalnummer = from_db_rstrip
        )
    event_types = \
        { 4.0   : 'delete'
        , 5.0   : 'insert'
        , 6.0   : 'update'
        }

    acc_status = \
        ( 'phonlineAccStBediensteter'
        , 'phonlineAccStStudent'
        , 'phonlineAccStWeiterbildung'
        )

    def __init__ (self, args) :
        self.args      = args
        # FIXME: Poor-mans logger for now
        self.log = Namespace ()
        self.log ['debug'] = log_debug
        self.log ['error'] = log_error
        self.log ['warn']  = log_warn
        self.log ['info']  = log_info
        self.ldap      = LDAP_Access (self.args, self)
        self.verbose ("Bound to ldap")
        self.table     = 'benutzer_alle_dirxml_v'
        self.crypto_iv = None
        if self.args.crypto_iv :
            self.crypto_iv = self.args.crypto_iv
        self.aes = AES_Cipher \
            (hexlify (self.args.encryption_password.encode ('utf-8')))
        self.get_passwords ()
        # copy class dict to local dict
        self.data_conversion = dict (self.data_conversion)
        # and add a bound method
        self.data_conversion ['passwort'] = self.from_password
        self.read_only = \
            dict ((r, datetime (2017, 1, 1)) for r in self.args.read_only)
        self.ph15dn = None
        self.ph15db = None
        if self.args.action == 'etl' :
            for n, dn in enumerate (self.args.base_dn) :
                if 'ph15' in dn :
                    self.ph15dn = dn
                    if len (self.args.databases) > n :
                        self.ph15db = self.args.databases [n]
                    elif (   len (self.args.databases) == 1
                         and self.args.databases [0] == 'postgres'
                         ) :
                        self.ph15db = self.args.databases [0]
                    break
        self.do_sleep = True
        # We do not get events when cn changes for ph15, so we put the
        # into this dict and use it to sync ph15 with it (only the event
        # is used, not the actual change)
        self.ph15_change_dn = {}
    # end def __init__

    def action (self) :
        if self.args.action == 'initial_load' :
            self.initial_load ()
        elif self.args.action == 'etl' :
            while True :
                open ('/tmp/liveness', 'w').close ()
                for dn, db in zip (self.args.base_dn, self.args.databases) :
                    self.db = db
                    self.dn = dn
                    try :
                        self.verbose ("DB-Connect: %s %s" % (db, dn))
                        self.cnx    = pyodbc.connect (DSN = db)
                        self.cursor = self.cnx.cursor ()
                        self.verbose ("connected.")
                    except Exception as cause :
                        raise (ApplicationError (cause))
                    self.etl ()
                    self.cursor.close ()
                    self.cnx.close ()
                if self.ph15db :
                    self.db     = self.ph15db
                    self.dn     = self.ph15dn
                    self.cnx    = pyodbc.connect (DSN = self.db)
                    self.cursor = self.cnx.cursor ()
                    self.update_ph15_cn ()
                    self.cursor.close ()
                    self.cnx.close ()
                if self.do_sleep :
                    self.verbose ("Sleeping: %s" % self.args.sleeptime)
                    time.sleep (self.args.sleeptime)
                else :
                    self.verbose ("Not sleeping")
        else :
            raise ValueError ('Invalid action: %s' % self.args.action)
    # end def action

    def db_iter_part (self, count, start = 0, end = None) :
        fields = self.fields [self.table]
        sql    = 'select %s from %s where pk_uniqueid >= ?'
        params = [start]
        if end :
            sql += ' and pk_uniqueid < ?'
            params.append (end)
        self.cursor.execute \
            (sql  % (','.join (fields), self.table), *params)
        for n, row in enumerate (self.cursor) :
            yield ((n + count, row))
    # end def db_iter_part

    def db_iter (self, db) :
        self.cnx    = pyodbc.connect (DSN = db)
        self.cursor = self.cnx.cursor ()
        tbl         = self.table
        fields      = self.fields [tbl]
        if db.endswith ('15') :
            count = 0
            self.cursor.execute ('select pk_uniqueid from %s' % (tbl))
            uids = self.cursor.fetchall ()
            uids = tuple (u [0] for u in sorted (uids, key = lambda x : x [0]))
            last = 0
            for i in range (1000, len (uids), 1000) :
                for x, row in self.db_iter_part (count, last, uids [i]) :
                    yield ((x, row))
                count = x
                last  = uids [i]
            for x, row in self.db_iter_part (count, last) :
                yield ((x, row))
        else :
            self.cursor.execute ('select %s from %s' % (','.join (fields), tbl))
            # fetchall is *much* faster
            for n, row in enumerate (self.cursor.fetchall ()) :
                yield ((n, row))
    # end def db_iter

    def delete_in_ldap (self, pk_uniqueid) :
        uid = self.to_ldap (pk_uniqueid, 'pk_uniqueid')
        m   = []
        entries = self.ldap.get_entries (uid) [:]
        for ldrec in entries :
            dn = ldrec ['dn']
            r = self.ldap.delete (dn)
            self.verbose ("Deleting record: %s" % dn)
            if not r :
                msg = \
                    ( "Error on LDAP delete: "
                      "%(description)s: %(message)s"
                      " (code: %(result)s)"
                    % self.ldap.result
                    )
                self.log.error (msg)
                m.append (msg)
        # We check if any of the entries doesn't have an account anymore
        # If so we must delete it in ph15.
        if 'ph15' not in self.dn :
            for ldrec in entries :
                cn = ldrec ['attributes']['cn']
                if isinstance (cn, type ([])) :
                    assert len (cn) == 1
                    cn = cn [0]
                matches = self.ldap.search_cn_all (cn)
                # If we get no result or more than one we have nothing to do
                nm = len (matches)
                if not matches or nm > 2 or not nm :
                    self.verbose \
                        ("Not deleting cn=%s in ph15: found %s" % (cn, nm))
                    continue
                m = matches [0]
                if 'ph15' not in m ['dn'] :
                    self.log.error \
                        ( 'During deletion: Found CN=%s in DN=%s '
                          'but not in ph15'
                        % (cn, m ['dn'])
                        )
                    continue
                acc_status_found = False
                for a in self.acc_status :
                    if m ['attributes'].get (a) :
                        acc_status_found = True
                        break
                dn = 'cn=' + cn + ',' + self.dn15
                assert (dn == m ['dn'])
                if acc_status_found :
                    self.verbose ("Not deleting %s: has account" % dn)
                    continue
                r = self.ldap.delete (dn)
                if not r :
                    msg = \
                        ( "Error on LDAP delete ph15: "
                          "%(description)s: %(message)s"
                          " (code: %(result)s)"
                        % self.ldap.result
                        + "DN=%s" % dn
                        )
                    self.log.error (msg)
                    m.append (msg)
        if m :
            return '\n'.join (m)
    # end def delete_in_ldap

    def etl (self) :
        tbl    = 'eventlog_ph'
        fields = self.fields [tbl]
        if self.db in self.read_only :
            # Note: The limit below (max_records) interacts badly with
            # limiting the records by date: We don't see all records for
            # a certain date/time in a single run. So we would have to
            # keep the date and use different ranges of records (and
            # hope that nothing changes until the next run).
            max_evdate = self.read_only [self.db]
            sql  = "select %s from %s where event_time > "
            if self.db == 'postgres' :
                dtfun = 'to_timestamp'
            else :
                dtfun = 'to_date'
            sql += "%s('%s', 'YYYY-MM-DD.HH24:MI:SS')" \
                 % (dtfun, max_evdate.strftime ('%Y-%m-%d.%H:%M:%S'))
        else :
            sql = "select %s from %s where status in ('N', 'E')"
        if self.db == 'postgres' :
            sql += ' limit %s' % self.args.max_records
        else :
            sql += ' and rownum <= %s' % self.args.max_records
        sql = sql % (', '.join (fields), tbl)
        self.cursor.execute (sql)
        updates = {}
        rows = self.cursor.fetchall ()
        self.verbose ("Eventlog query done, %s rows" % len (rows))
        self.do_sleep = True
        if len (rows) >= self.args.max_records :
            self.do_sleep = False
        for row in rows :
            rw = Namespace ((k, row [i]) for i, k in enumerate (fields))
            self.verbose \
                ( "Eventlog id: %s type: %s status: %s in %s"
                % (rw.record_id, rw.event_type, rw.status, self.db)
                )
            if self.db in self.read_only and rw.event_time > max_evdate :
                max_evdate = rw.event_time
            if rw.event_type not in self.event_types :
                msg = 'Invalid event_type in %s: %s' % (self.db, rw.event_type)
                updates [rw.record_id] = dict \
                    ( error_message = msg
                    , status        = 'F'
                    )
                self.error (msg)
                continue
            event_type = self.event_types [rw.event_type]
            if not rw.table_key.startswith ('pk_uniqueid=') :
                msg = 'Invalid table_key in %s, expect pk_uniqueid=' % self.db
                updates [rw.record_id] = dict \
                    ( error_message = msg
                    , status        = 'F'
                    )
                self.error (msg)
                continue
            if rw.table_name.lower () != 'benutzer_alle_dirxml_v' :
                msg  = 'Invalid table_name in %s, expect benutzer_alle_dirxml_v'
                msg %= self.db
                updates [rw.record_id] = dict \
                    ( error_message = msg
                    , status        = 'F'
                    )
                self.error (msg)
                continue
            uid = rw.table_key.split ('=', 1) [-1]
            try :
                uid = int (uid)
            except ValueError :
                msg  = 'Invalid table_key: %s in %s, expect numeric id'
                msg %= (uid, self.db)
                updates [rw.record_id] = dict \
                    ( error_message = msg
                    , status        = 'F'
                    )
                self.error (msg)
                continue
            self.warning_message = None
            sql = 'select %s from %s where pk_uniqueid = ?'
            sql = sql % (','.join (self.fields [self.table]), self.table)
            self.cursor.execute (sql, uid)
            usr = self.cursor.fetchall ()
            if len (usr) > 1 :
                msg = "Duplicate pk_uniqueid: %s in %s" % (uid, self.db)
                updates [rw.record_id] = dict \
                    ( error_message = msg
                    , status        = 'W'
                    )
                self.log.warn (msg)
            if len (usr) :
                if event_type == 'delete' :
                    msg = 'Record %s existing in DB %s' % (uid, self.db)
                    updates [rw.record_id] = dict \
                        ( error_message = msg
                        , status        = 'W'
                        )
                    self.log.warn (msg)
                is_new = event_type == 'insert'
                msg = []
                for usr_row in usr :
                    m = self.sync_to_ldap (usr_row, is_new = is_new)
                    if m :
                        msg.append (m)
                msg = '\n'.join (msg)
            else :
                if event_type != 'delete' :
                    msg = 'Record %s not existing in DB' % uid
                    updates [rw.record_id] = dict \
                        ( error_message = msg
                        , status        = 'W'
                        )
                    self.log.warn (msg)
                msg = self.delete_in_ldap (uid)
            if msg :
                # Error message, overwrite possible earlier warnings for
                # this record
                status  = 'E'
                attempt = int (rw.attempt)
                if attempt > 10 :
                    status = 'F'
                attempt += 1
                updates [rw.record_id] = dict \
                    ( error_message = msg
                    , status        = status
                    , attempt       = attempt
                    )
            elif self.warning_message :
                if rw.record_id in updates :
                    assert updates [rw.record_id][status] == 'W'
                    updates [rw.record_id]['error_message'] = '\n'.join \
                        (( updates [rw.record_id]['error_message']
                         , self.warning_message
                        ))
                else :
                    updates [rw.record_id] = dict \
                        ( error_message = self.warning_message
                        , status        = 'W'
                        )
            elif rw.record_id in updates :
                pass
            else :
                updates [rw.record_id] = dict (status = 'S')
            updates [rw.record_id]['read_time'] = datetime.utcnow ()
        if self.db in self.read_only :
            self.read_only [self.db] = max_evdate
            self.verbose ("Not updating eventlog")
        else :
            for key in updates :
                fn  = list (sorted (updates [key].keys ()))
                sql = "update eventlog_ph set %s where record_id = ?"
                sql = sql % ', '.join ('%s = ?' % k for k in fn)
                #print (sql)
                p   = list (updates [key][k] for k in fn)
                p.append (float (key))
                #print (p)
                self.cursor.execute (sql, * p)
            self.cursor.commit ()
    # end def etl

    def update_ph15_cn (self) :
        """ Special case for ph15: We process the triggers of changed CNs
            for other databases
        """
        if 'ph15' in self.dn and self.ph15_change_dn :
            sql = 'select %s from %s where benutzername in (?, ?)'
            sql = sql % (','.join (self.fields [self.table]), self.table)
            for oldcn in self.ph15_change_dn :
                newcn = self.ph15_change_dn [oldcn]
                self.cursor.execute (sql, oldcn, newcn)
                rows = self.cursor.fetchall ()
                if len (rows) > 1 :
                    self.log.warn \
                        ( 'Duplicate CN on cn change ph15: "%s/%s": %s'
                        % (oldcn, newcn, len (rows))
                        )
                for row in rows :
                    self.sync_to_ldap (row, is_new = False)
        self.ph15_change_dn = {}
    # end def update_ph15_cn
#                    if len (rows) :
#                        if cn == oldcn :
#                            self.log.warn \
#                                ('CN change ph15: "%s" still in DB' % cn)
#                        if len (rows > 1) :
#                            self.log.warn ('Duplicate CN: "%s"' % cn)
#                        for row in rows :
#                            self.sync_to_ldap (row, is_new = False)
#                    else :
#                        if cn == newcn :
#                            self.log.warn \
#                                ('CN change ph15: "%s" not in DB' % cn)
#                        dn = 'cn=%s,' % cn + self.dn
#                        self.verbose ("Deleting record: %s" % dn)
#                        r = self.ldap.delete (dn)
#                        if not r :
#                            msg = \
#                                ( "Error on LDAP delete: "
#                                  "%(description)s: %(message)s"
#                                  " (code: %(result)s)"
#                                % self.ldap.result + "DN=%s" % dn
#                                )
#                            self.log.error (msg)

    def generate_initial_tree (self) :
        """ Check if initial tree exists, generate if non-existing
        """
        for dn in self.args.base_dn :
            rdn_lists = []
            spdn = dn.split (',')
            rdn_lists.append (spdn)
            if spdn [0] == 'ou=user' :
                rdn_lists.append (['ou=ETD', 'ou=idnSync'] + spdn [1:])
            for rdns in rdn_lists :
                self.generate_rdns (rdns)
    # end def generate_initial_tree

    def generate_rdns (self, rdns) :
        """ Generate a top-down list of RDNs
        """
        top = None
        bdn = ''
        for dn in reversed (rdns) :
            if top is None :
                top = dn
            if bdn :
                bdn = ','.join ((dn, bdn))
            else :
                bdn = dn
            entry = self.ldap.get_by_dn (bdn)
            k, v  = dn.split ('=', 1)
            if entry :
                assert entry ['attributes'][k] in (v, [v])
                continue
            d = {k : v}
            if k == 'o' :
                d ['objectClass'] = 'Organization'
            else :
                d ['objectClass'] = 'organizationalUnit'
            r = self.ldap.add (bdn, attributes = d)
            if not r :
                msg = \
                    ( "Error on LDAP add: "
                      "%(description)s: %(message)s"
                      " (code: %(result)s)"
                    % self.ldap.result
                    )
                self.log.error (msg)
                self.log.error ("DN: %s, Attributes were: %s" % (bdn, d))
    # end def generate_rdns

    def get_passwords (self) :
        self.passwords = dict ()
        try :
            with open ('/etc/conf/passwords', 'r') as f :
                for line in f :
                    line = line.strip ()
                    if line.startswith ('DATABASE_PASSWORDS') :
                        pws = line.split ('=', 1)[-1].strip ()
                        for entry in pws.split (',') :
                            db, pw = (x.strip () for x in entry.split (':', 1))
                            self.passwords [db] = pw
        except FileNotFoundError :
            return
    # end def get_passwords

    def initial_load (self) :
        self.generate_initial_tree ()
        tbl     = self.table
        fields  = self.fields [tbl]
        for bdn, db in zip (self.args.base_dn, self.args.databases) :
            self.db = db
            self.dn = bdn
            self.log.debug ("%s: %s" % (db, self.dn))
            # Get all unique ids currently in ldap under our tree
            self.uidmap = {}
            r = self.ldap.search \
                ( self.dn, '(phonlineUniqueId=*)'
                , search_scope = LEVEL
                , attributes   = ['phonlineUniqueId']
                )
            if r :
                for entry in self.ldap.response :
                    uid = entry ['attributes']['phonlineUniqueId']
                    self.uidmap [uid] = entry ['dn']
                    assert entry ['dn'].endswith (self.dn)
            for n, row in self.db_iter (db) :
                if (n % 1000) == 0 or self.args.verbose :
                    self.log.debug (n)
                idx = fields.index ('pk_uniqueid')
                uid = "%d" % row [idx]
                if uid in self.uidmap :
                    del self.uidmap [uid]
                self.sync_to_ldap (row, is_new = True)
            for u in sorted (self.uidmap) :
                udn = self.uidmap [u]
                self.log.warn ("Deleting: %s: %s" % (u, udn))
                r = self.ldap.delete (udn)
                if not r :
                    msg = \
                        ( "Error on LDAP delete: "
                          "%(description)s: %(message)s"
                          " (code: %(result)s)"
                        % self.ldap.result
                        )
                    self.log.error (msg)
        self.log.info ("SUCCESS")
        sys.stdout.flush ()
        # Default is to wait forever after initial load
        if not self.args.terminate :
            while True :
                time.sleep (self.args.sleeptime)
    # end def initial_load

    def sync_to_ldap (self, row, is_new = False) :
        """ Sync a single record to LDAP. We return an error message if
            something goes wrong (and log the error). The caller might
            want to put the error message into some table in the
            database.
        """
        timestamp = LdapTimeStamp (datetime.now (pytz.utc))
        etl_ts = timestamp.as_generalized_time ()
        tbl = self.table
        rw  = Namespace ((k, row [i]) for i, k in enumerate (self.fields [tbl]))
        if not rw.get ('benutzername') :
            self.log.error \
                ( "Got User without benutzername, pk_uniqueid=%s"
                % rw.get ('pk_uniqueid')
                )
            return
        if not rw.get ('pk_uniqueid') :
            self.log.error \
                ( "Got User without pk_uniqueid, benutzername=%s"
                % rw.get ('benutzername')
                )
            return
        uid   = self.to_ldap (rw.pk_uniqueid, 'pk_uniqueid')
        # Find cn in LDAP phonlineUniqueId
        ldrec = self.ldap.get_by_cn (rw.benutzername)
        if not ldrec :
            # Try matching by pk_uniqueid
            ldr = self.ldap.get_entries (uid)
            if ldr and len (ldr) > 1 :
                msg = \
                    ( "Non-matching cn: %s and more than one record"
                      " with same pk_uniqueid: %s, giving up"
                    % (rw.benutzername, uid)
                    )
                self.log.error (msg)
                return msg
            elif ldr and len (ldr) == 1 :
                ldrec = ldr [0]
        if ldrec :
            if is_new :
                # Log a warning but continue like a normal sync
                # During initial_load issue warning only if verbose
                msg = 'Found dn "%s" when sync says it should be new' \
                    % ldrec ['dn']
                if self.args.verbose or self.args.action != 'initial_load' :
                    self.log.warn (msg)
                self.warning_message = msg
            nuid = ldrec ['attributes'].get ('phonlineUniqueId')
            if nuid != uid :
                msg = \
                    ( 'Found dn: %s with different phonlineUniqueId: '
                      'Got %s, expected %s'
                    % (ldrec ['dn'], nuid, uid)
                    )
                self.log.warn (msg)
                self.warning_message = msg
            # Ensure we use the same IV for comparison
            pw = ldrec ['attributes'].get ('idnDistributionPassword', '')
            if len (pw) > 32 :
                self.crypto_iv = pw [:32]
            ld_update = {}
            ld_delete = {}
            for k in rw :
                v  = self.to_ldap (rw [k], k)
                lk = self.odbc_to_ldap_field [k]
                lv = ldrec ['attributes'].get (lk, None)
                if v == lv or [v] == lv :
                    continue
                if v is None :
                    ld_delete [lk] = None
                else :
                    # Ensure we use new random IV if pw changes
                    # We've used the IV of the old password for
                    # comparison previously
                    if k == 'passwort' :
                        self.crypto_iv = self.args.crypto_iv
                        v = self.to_ldap (rw [k], k)
                    ld_update [lk] = v
            assert 'phonlineUniqueId' not in ld_delete
            if not ld_delete and not ld_update :
                return
            ld_update ['etlTimestamp'] = etl_ts
            # dn modified, the cn is the rdn!
            dn = ldrec ['dn']
            if 'cn' in ld_update :
                oldcn = ldrec ['attributes']['cn']
                if isinstance (oldcn, type ([])) :
                    assert len (oldcn) == 1
                    oldcn = oldcn [0]
                self.ph15_change_dn [oldcn] = ld_update ['cn']
                cn = 'cn=' + ld_update ['cn']
                r  = self.ldap.modify_dn (ldrec ['dn'], cn)
                if not r :
                    msg = \
                        ( "Error on LDAP modify_dn: "
                          "%(description)s: %(message)s"
                          " (code: %(result)s)"
                        % self.ldap.result
                        )
                    self.log.error (msg)
                    return msg
                del ld_update ['cn']
                ndn = cn + ',' + dn.split (',', 1)[-1]
                self.verbose ("Change dn: %s->%s" % (dn, ndn))
                dn = ndn
            ph15changes = {}
            if 'idnDistributionPassword' in ld_update :
                ph15changes ['passwort'] = True
                self.verbose ("Change password for dn: %s" % dn)
                self.ldap.extend.standard.modify_password \
                    (dn, new_password = rw ['passwort'].encode ('utf-8'))
            for ph15k in 'vorname', 'nachname', 'emailadresse_st' :
                if self.odbc_to_ldap_field [ph15k] in ld_update :
                    ph15changes [ph15k] = True
            if ph15changes :
                cn = dn.split (',')[0]
                self.update_attributes_ph15 (cn, uid, rw, ph15changes)
            if ld_update or ld_delete :
                changes = {}
                for k in ld_update :
                    if isinstance (ld_update [k], type ([])) :
                        changes [k] = (MODIFY_REPLACE, ld_update [k])
                    else :
                        changes [k] = (MODIFY_REPLACE, [ld_update [k]])
                    self.verbose ("Change %s for dn: %s" % (k, dn))
                for k in ld_delete :
                    changes [k] = (MODIFY_DELETE, [])
                    self.verbose ("Delete %s in dn: %s" % (k, dn))
                r = self.ldap.modify (dn, changes)
                if not r :
                    msg = \
                        ( "Error on LDAP modify: "
                          "%(description)s: %(message)s"
                          " (code: %(result)s)"
                        % self.ldap.result
                        )
                    self.log.error (msg + str (changes))
                    return msg
        else :
            # Ensure we use new random IV if pw changes
            # We've used the IV of the old password for
            # comparison previously
            self.crypto_iv = self.args.crypto_iv
            if not is_new :
                # Log a warning but continue like a normal sync
                msg = 'pk_uniqueid "%s" not found, sync says it exists' % uid
                self.log.warn (msg)
                self.warning_message = msg
            ld_update = {}
            for k in rw :
                lk = self.odbc_to_ldap_field [k]
                v  = self.to_ldap (rw [k], k)
                if v is not None :
                    ld_update [lk] = v
            ld_update ['objectClass'] = \
                ['inetOrgPerson', 'phonlinePerson','idnSyncstat']
            ld_update ['etlTimestamp'] = etl_ts
            dn = ('cn=%s,' % ld_update ['cn']) + self.dn
            r  = self.ldap.add (dn, attributes = ld_update)
            self.verbose ("Adding dn: %s" % dn)
            if not r :
                msg = \
                    ( "Error on LDAP add: %(description)s: %(message)s"
                      " (code: %(result)s) "
                    % self.ldap.result
                    ) + 'DN: %s, uid: %s, attributes: %s' % (dn, uid, ld_update)
                self.log.error (msg)
                return msg
            if 'idnDistributionPassword' in ld_update :
                self.ldap.extend.standard.modify_password \
                    (dn, new_password = rw ['passwort'].encode ('utf-8'))
            self.create_record_ph15 (uid, rw, ld_update)
    # end def sync_to_ldap

    def update_attributes_ph15 (self, cn, uid, rw, chkeys) :
        """ Write attributes through to ph15 if attribute changes on
            another instance: Sync of the database may take too long so
            we optimize this for some attribute changes. Note that for
            many changes in ph15 we will never get an explicit event.
        """
        # For initial load we don't write to other instances:
        if self.args.action != 'etl' :
            return
        # If we're working on ph15 now or no ph15: nothing to do
        if not self.ph15dn or 'ph15' in self.dn :
            return
        ldrec = self.ldap.get_by_cn (cn, self.dn15)
        # If record doesn't exist in ph15 we do nothing
        if not ldrec :
            self.log.warn ("CN %s not in ph15" % cn)
            return
        dn = ldrec ['dn']
        changes = {}
        for k in chkeys :
            if k == 'passwort' :
                password = rw [k]
                self.ldap.extend.standard.modify_password \
                    (dn, new_password = password.encode ('utf-8'))
                self.crypto_iv = self.args.crypto_iv
                v = self.to_ldap (password, 'passwort')
                changes ['idnDistributionPassword'] = (MODIFY_REPLACE, v)
            else :
                changes 
                v  = self.to_ldap (rw [k], k)
                # Don't delete attribute in ph15
                if v is None :
                    continue
                lk = self.odbc_to_ldap_field [k]
                lv = ldrec ['attributes'].get (lk, None)
                if v == lv or [v] == lv :
                    continue
                self.verbose ("Change %s for dn: %s" % (lk, dn))
                if isinstance (v, type ([])) :
                    changes [lk] = (MODIFY_REPLACE, v)
                else :
                    changes [lk] = (MODIFY_REPLACE, [v])
        r = self.ldap.modify (dn, changes)
        if not r :
            msg = \
                ( "Error on LDAP modify (password ph15): "
                  "%(description)s: %(message)s"
                  " (code: %(result)s)"
                % self.ldap.result
                )
            self.log.error (msg + str (change))
        self.verbose ("Changed password for %s" % dn)
    # end def update_attributes_ph15

    def create_record_ph15 (self, uid, rw, ld_update) :
        # For now disabled: The uid is different in both instances so
        # there is no way to write this through.
        return
        # For initial load we don't write to other instances:
        if self.args.action != 'etl' :
            return
        # If we're working on ph15 now or no ph15: nothing to do
        if not self.ph15dn or 'ph15' in self.dn :
            return
        # FIXME: If we ever enable this again, this should use
        # get_entries and be aware that there may be more than one
        ldrec = self.ldap.get_entry (uid, dn = self.dn15)
        if ldrec :
            self.log.warn ("Uid %s already in ph15" % uid)
            return
        dn = ('cn=%s,' % ld_update ['cn']) + self.dn15
        r  = self.ldap.add (dn, attributes = ld_update)
        self.verbose ("Adding dn: %s" % dn)
        if not r :
            msg = \
                ( "Error on LDAP add: %(description)s: %(message)s"
                  " (code: %(result)s) "
                % self.ldap.result
                ) + 'DN: %s, uid: %s, attributes: %s' % (dn, uid, ld_update)
            self.log.error (msg)
            return msg
        if 'idnDistributionPassword' in ld_update :
            self.ldap.extend.standard.modify_password \
                (dn, new_password = rw ['passwort'].encode ('utf-8'))
    # end def create_record_ph15

    def to_ldap (self, item, dbkey) :
        conv = self.data_conversion.get (dbkey)
        if conv :
            return conv (item)
        return item
    # end def to_ldap

    def from_password (self, item) :
        """ Return encrypted password
        """
        iv = None
        if self.crypto_iv :
            iv = unhexlify (self.crypto_iv)
        return self.aes.encrypt (item.encode ('utf-8'), iv).decode ('ascii')
    # end def from_password

    def verbose (self, msg) :
        """ Verbose message for etl sync only
        """
        if self.args.verbose and self.args.action != 'initial_load' :
            self.log.debug (msg)
    # end def verbose

    @property
    def dn15 (self) :
        offset = self.dn.index ('ou=ph')
        assert (offset > 0)
        return self.dn [0:offset] + 'ou=ph15' + self.dn [offset+7:]
    # end def dn15

# end class ODBC_Connector

def main () :
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( 'action'
        , help    = 'Action to perform, one of "initial_load", "etl"'
        )
    default_bind_dn = os.environ.get ('LDAP_BIND_DN', 'cn=admin,o=BMUKK')
    cmd.add_argument \
        ( "-B", "--bind-dn"
        , help    = "Bind-DN, default=%(default)s"
        , default = default_bind_dn
        )
    cmd.add_argument \
        ( "-c", "--database-connect"
        , dest    = 'databases'
        , help    = "Database name for connecting usually configured via "
                    "environment, will use *all* databases specified"
        , action  = 'append'
        , default = []
        )
    cmd.add_argument \
        ( "-d", "--base-dn"
        , help    = "Base-DN for starting search, usually configured via "
                    "environment, will use *all* databases specified"
        , action  = 'append'
        , default = []
        )
    cmd.add_argument \
        ( "-i", "--crypto-iv"
        , help    = "You can pass in a fixed crypto initialisation vector"
                    " for regression testing -- don't do this in production!"
        )
    cmd.add_argument \
        ( '-m', '--max-records'
        , help    = "Maximum number of records per etl run"
        , type    = int
        , default = 100
        )
    cmd.add_argument \
        ( '-o', '--output-file'
        , help    = 'Output file for writing CSV, default is table name'
        )
    # Get default_pw from /etc/conf/passwords LDAP_PASSWORD entry.
    # Also get password-encryption password when we're at it
    ldap_pw = 'changeme'
    pw_encr = 'changemetoo*****' # must be 16 characters long after encoding
    try :
        with open ('/etc/conf/passwords', 'r') as f :
            for line in f :
                if line.startswith ('LDAP_PASSWORD') :
                    ldap_pw = line.split ('=', 1) [-1].strip ()
                if line.startswith ('PASSWORD_ENCRYPTION_PASSWORD') :
                    pw_encr = line.split ('=', 1) [-1].strip ()
    except FileNotFoundError :
        pass
    cmd.add_argument \
        ( "-P", "--password"
        , help    = "Password(s) for binding to LDAP"
        , default = ldap_pw
        )
    cmd.add_argument \
        ( "-p", "--encryption-password"
        , help    = "Password(s) for encrypting passwords in LDAP"
        , default = pw_encr
        )
    cmd.add_argument \
        ( "-r", "--read-only"
        , help    = "Databases given with this options will not write eventlog"
        , default = []
        , action  = 'append'
        )
    sleeptime = int (os.environ.get ('ETL_SLEEPTIME', '20'))
    cmd.add_argument \
        ( '-s', '--sleeptime'
        , help    = "Seconds to sleep between etl invocations, "
                    " default=%(default)s"
        , type    = int
        , default = sleeptime
        )
    cmd.add_argument \
        ( '-t', '--terminate'
        , help    = "Terminate container after initial_load"
        , action  = "store_true"
        , default = False
        )
    cmd.add_argument \
        ( '-v', '--verbose'
        , help    = "Verbose logging"
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
    if not args.base_dn or not args.databases :
        args.base_dn   = []
        args.databases = []
        for inst in os.environ ['DATABASE_INSTANCES'].split (',') :
            db, dummy = (x.strip () for x in inst.split (':'))
            dn = ','.join \
                (( os.environ ['LDAP_USER_OU']
                ,  'ou=%s' % db
                ,  os.environ ['LDAP_BASE_DN']
                ))
            args.base_dn.append   (dn)
            args.databases.append (db)
    for db in args.read_only :
        if db not in args.databases :
            raise ApplicationError ("Invalid Database in read-only: %s" % db)

    odbc = ODBC_Connector (args)
    try :
        odbc.action ()
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
