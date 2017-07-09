#!/usr/bin/python3

import pyodbc
from   argparse import ArgumentParser
from   csv      import writer

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

    def __init__ (self, args) :
        self.cnx    = pyodbc.connect (DSN = 'oracle')
        self.cursor = self.cnx.cursor ()
        self.args   = args
        self.table  = args.table.lower ()
    # end def __init__

    def as_csv (self) :
        """ Get table into a csv file. If a time is given, we only
            select the relevant table rows from the eventlog_ph table
            and then select the relevant rows from the
            benutzer_alle_dirxml_v table.
        """
        fields = self.fields [self.table]
        where  = ''
        fn     = self.args.output_file
        if not fn :
            fn = self.table
        if self.args.time :
            t     = self.args.time.replace ('.', ' ')
            fmt   = 'YYYY-MM-DD HH:MI:SS'
            where = "where event_time > to_date ('%s', '%s')" % (t, fmt)
            fn    = fn + '.' + self.args.time.replace (' ', '.')
        if not fn.endswith ('.csv') :
            fn = fn + '.csv'
        ids = []
        with open (fn, 'w', encoding = 'utf-8') as f :
            w = writer (f, delimiter = self.args.delimiter)
            w.writerow (fields)
            self.cursor.execute \
                ( 'select %s from %s %s'
                % (','.join (fields), self.table, where)
                )
            for row in self.cursor :
                w.writerow (row)
                ids.append (row [1].split ('=') [1])
        if self.args.time and ids :
            tbl = 'benutzer_alle_dirxml_v'
            fn  = tbl + '.' + self.args.time.replace (' ', '.') + '.csv'
            fields = self.fields [tbl]
            where  = "where pk_uniqueid in (%s)" % ','.join (ids)
            with open (fn, 'w', encoding = 'utf-8') as f :
                w = writer (f, delimiter = self.args.delimiter)
                w.writerow (fields)
                self.cursor.execute \
                    ( 'select %s from %s %s'
                    % (','.join (fields), tbl, where)
                    )
                for row in self.cursor :
                    w.writerow (row)
    # end def as_csv

# end class ODBC_Connector

def main () :
    cmd = ArgumentParser ()
    cmd.add_argument \
        ( '-d', '--delimiter'
        , help    = 'Delimiter of csv file, default=%(default)s'
        , default = ';'
        )
    cmd.add_argument \
        ( '-o', '--output-file'
        , help    = 'Output file for writing CSV, default is table name'
        )
    cmd.add_argument \
        ( '-t', '--table'
        , help    = 'Table name, default=%(default)s'
        , default = 'eventlog_ph'
        )
    cmd.add_argument \
        ( '-T', '--time'
        , help    = 'Cut-off time for eventlog, if specified, both the '
                    'eventlog_ph *and* the benutzer_alle_dirxml_v are '
                    'dumped but only records newer than the given time.'
        )
    args = cmd.parse_args ()
    odbc = ODBC_Connector (args)
    odbc.as_csv ()
# end def main

if __name__ == '__main__' :
    main ()
