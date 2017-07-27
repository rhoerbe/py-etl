#!/usr/bin/python3

import pyodbc
from   argparse import ArgumentParser
from   csv      import DictReader

class ODBC_Connector (object) :

    fields = dict \
        ( benutzer_alle_dirxml_v = dict
            ( person_nr_obf         = 'varchar(4000)'
            , st_person_nr_obf      = 'varchar(4000)'
            , org_einheiten         = 'varchar(4000)'
            , emailadresse_b        = 'varchar(4000)'
            , emailadresse_st       = 'varchar(4000)'
            , bpk                   = 'varchar(84)'
            , pm_sap_personalnummer = 'varchar(4000)'
            , schulkennzahlen       = 'varchar(4000)'
            , funktionen            = 'varchar(4000)'
            , pk_uniqueid           = 'double precision'
            , vorname               = 'varchar(4000)'
            , nachname              = 'varchar(4000)'
            , benutzername          = 'varchar(4000)'
            , passwort              = 'varchar(4000)'
            , benutzergruppen       = 'varchar(4000)'
            , aktiv_st_person       = 'char(3)'
            , aktiv_a_person        = 'char(3)'
            , aktiv_b_person        = 'char(3)'
            , chipid_b              = 'varchar(150)'
            , chipid_st             = 'varchar(150)'
            , chipid_a              = 'varchar(150)'
            , mirfareid_b           = 'varchar(150)'
            , mirfareid_st          = 'varchar(150)'
            , mirfareid_a           = 'varchar(150)'
            , matrikelnummer        = 'varchar(4000)'
            , account_status_b      = 'varchar(4000)'
            , account_status_st     = 'varchar(4000)'
            , account_status_a      = 'varchar(4000)'
            , geburtsdatum          = 'timestamp(0)'
            , person_nr             = 'double precision'
            , st_person_nr          = 'double precision'
            , ident_nr              = 'double precision'
            )
        , eventlog_ph = dict
            ( record_id             = 'double precision'
            , table_key             = 'varchar(144)'
            , status                = 'char(3)'
            , event_type            = 'double precision'
            , event_time            = 'timestamp(0)'
            , perpetrator           = 'varchar(96)'
            , table_name            = 'varchar(96)'
            , column_name           = 'varchar(96)'
            , old_value             = 'varchar(240)'
            , new_value             = 'varchar(240)'
            , synch_id              = 'double precision'
            , synch_online_flag     = 'char(3)'
            , transaction_flag      = 'char(3)'
            , read_time             = 'timestamp(0)'
            , error_message         = 'varchar(4000)'
            , attempt               = 'double precision'
            , admin_notify_flag     = 'char(3)'
            )
        )
    primary_key = dict \
        ( benutzer_alle_dirxml_v = 'pk_uniqueid'
        , eventlog_ph            = 'record_id'
        )

    def __init__ (self, args) :
        self.args   = args
        self.cnx    = pyodbc.connect (DSN = args.database)
        self.cursor = self.cnx.cursor ()
    # end def __init__

    # functions starting with cmd_ implement external commands
    # to be called via command-line interface

    def cmd_initial_load (self) :
        self.drop_tables ()
        self.create_tables ()
        self.load_initial_testdata ()
    # end def cmd_initial_load

    def cmd_update (self, update_number) :
        self.update_data ('testdata/changeset%s.csv' % update_number)
        self.load_data \
            ('testdata/eventlog%s.csv' % update_number, 'eventlog_ph')
    # end def cmd_update

    def create_tables (self) :
        """ Create the tables in the given database
        """
        for tbl in self.fields :
            f   = self.fields [tbl]
            p   = self.primary_key [tbl]
            sql = \
                ( 'create table %s (%s, primary key (%s))'
                % (tbl, ', '.join ('%s %s' % (k, v) for k, v in f.items ()), p)
                )
            #print (sql)
            try :
                self.cursor.execute (sql)
            except pyodbc.Error as cause :
                if 'already exists' not in str (cause) :
                    raise
    # end def create_tables

    def drop_tables (self) :
        for tbl in self.fields :
            sql = 'drop table %s' % tbl
            self.cursor.execute (sql)
    # end def drop_tables

    def insert (self, table, d) :
        fn  = sorted (d.keys ())
        sql = 'insert into %s (%s) values (%s)'
        sql = \
            ( sql
            % ( table
              , ','.join (k for k in fn if d [k])
              , ','.join (self.to_sql_tpl (k) for k in fn if d [k])
              )
            )
        v   = (self.to_sql (k, d [k]) for k in fn if d [k])
        self.cursor.execute (sql, *v)
    # end def insert

    def update (self, table, key, kvalue, d) :
        fn  = sorted (d.keys ())
        sql = 'update %s set %s where %s = ?'
        sql = \
            ( sql
            % ( table
              , ', '.join
                  ('%s = %s' % (k, self.to_sql_tpl (k, d [k])) for k in fn)
              , key
              )
            )
        v   = list (self.to_sql (k, d [k]) for k in fn if d [k])
        #print (sql, v)
        v.append (kvalue)
        self.cursor.execute (sql, *v)
    # end def update

    def load_data (self, filename, table) :
        with open (filename, 'r', encoding = 'utf-8') as f :
            dr = DictReader (f, delimiter = ';')
            for d in dr :
                self.insert (table, d)
        self.cursor.commit ()
    # end def load_data

    def load_initial_testdata (self) :
        self.load_data ('testdata/initial_data.csv', 'benutzer_alle_dirxml_v')
    # end def load_initial_testdata

    def to_sql (self, k, v) :
        if not v :
            return None
        for f in self.fields :
            if self.fields [f].get (k) == 'double precision' :
                return float (v)
        return v
    # end def to_sql

    def to_sql_tpl (self, k, v = 1) :
        if not v :
            return 'NULL'
        for sk in self.fields :
            if self.fields [sk].get (k, '').startswith ('timestamp') :
                # Code for oracle
                #return "to_date (?, 'YYYY-MM-DD HH:MI:SS')"
                # We're using postgres for testing, so use to_timestamp
                # We also need 24 hours clock, otherwise we get an error
                # message that 0 is not allowed for a 12 hour clock.
                return "to_timestamp (?, 'YYYY-MM-DD HH24:MI:SS')"
        return '?'
    # end def to_sql_tpl

    def update_data (self, filename) :
        """ Updates to existing records only for benutzer_alle_dirxml_v
            table, we have a hard-coded pk_uniqueid column in the code
        """
        table = 'benutzer_alle_dirxml_v'
        with open (filename, 'r', encoding = 'utf-8') as f :
            dr = DictReader (f, delimiter = ';')
            for d in dr :
                uid = d ['pk_uniqueid']
                sql = 'select pk_uniqueid from %s where pk_uniqueid = ?'
                sql = sql % table
                self.cursor.execute (sql, float (uid))
                row = self.cursor.fetchone ()
                #print ("Got: %s" % row)
                if row :
                    self.update (table, 'pk_uniqueid', float (uid), d)
                else :
                    self.insert (table, d)
        self.cursor.commit ()
    # end def update_data

# end class ODBC_Connector

def main () :
    commands = []
    for k in ODBC_Connector.__dict__ :
        if k.startswith ('cmd_') :
            commands.append (k [4:])
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( 'command'
        , help    = 'Command to execute, one of %s' % ', '.join (commands)
        )
    cmd.add_argument \
        ( '-A', '--argument'
        , help    = 'Argument to command'
        , default = []
        , action  = 'append'
        )
    cmd.add_argument \
        ( '-D', '--database'
        , help    = 'Database to connect to'
        , default = 'postgres'
        )
    args = cmd.parse_args ()
    odbc = ODBC_Connector (args)
    fun  = getattr (odbc, 'cmd_' + args.command)
    fun (* args.argument)
# end def main

if __name__ == '__main__' :
    main ()
