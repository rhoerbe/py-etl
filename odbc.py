#!/usr/bin/python3

import pyodbc
from   argparse import ArgumentParser
from   csv      import writer

class ODBC_Connector (object) :

    fields = dict \
        ( BENUTZER_ALLE_DIRXML_V =
            ( 'PERSON_NR_OBF'
            , 'ST_PERSON_NR_OBF'
            , 'ORG_EINHEITEN'
            , 'EMAILADRESSE_B'
            , 'EMAILADRESSE_ST'
            , 'BPK'
            , 'PM_SAP_PERSONALNUMMER'
            , 'SCHULKENNZAHLEN'
            , 'FUNKTIONEN'
            , 'PK_UNIQUEID'
            , 'VORNAME'
            , 'NACHNAME'
            , 'BENUTZERNAME'
            , 'PASSWORT'
            , 'BENUTZERGRUPPEN'
            , 'AKTIV_ST_PERSON'
            , 'AKTIV_A_PERSON'
            , 'AKTIV_B_PERSON'
            , 'CHIPID_B'
            , 'CHIPID_ST'
            , 'CHIPID_A'
            , 'MIRFAREID_B'
            , 'MIRFAREID_ST'
            , 'MIRFAREID_A'
            , 'MATRIKELNUMMER'
            , 'ACCOUNT_STATUS_B'
            , 'ACCOUNT_STATUS_ST'
            , 'ACCOUNT_STATUS_A'
            , 'GEBURTSDATUM'
            , 'PERSON_NR'
            , 'ST_PERSON_NR'
            , 'IDENT_NR'
            )
        , EVENTLOG_PH =
            ( 'RECORD_ID'
            , 'TABLE_KEY'
            , 'STATUS'
            , 'EVENT_TYPE'
            , 'EVENT_TIME'
            , 'PERPETRATOR'
            , 'TABLE_NAME'
            , 'COLUMN_NAME'
            , 'OLD_VALUE'
            , 'NEW_VALUE'
            , 'SYNCH_ID'
            , 'SYNCH_ONLINE_FLAG'
            , 'TRANSACTION_FLAG'
            , 'READ_TIME'
            , 'ERROR_MESSAGE'
            , 'ATTEMPT'
            , 'ADMIN_NOTIFY_FLAG'
            )
        )

    def __init__ (self, args) :
        self.cnx    = pyodbc.connect (DSN = 'oracle')
        self.cursor = self.cnx.cursor ()
        self.args   = args
        self.table  = args.table
    # end def __init__

    def as_csv (self) :
        fields = self.fields [self.table]
        with open (self.args.output_file, 'w', encoding = 'utf-8') as f :
            w = writer (f, delimiter = self.args.delimiter)
            w.writerow (fields)
            self.cursor.execute \
                ('select %s from %s' % (','.join (fields), self.table))
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
        , help    = 'Output file for writing CSV, default=%(default)s'
        , default = 'odbc-out.csv'
        )
    cmd.add_argument \
        ( '-t', '--table'
        , help    = 'Table name, default=%(default)s'
        , default = 'EVENTLOG_PH'
        )
    args = cmd.parse_args ()
    odbc = ODBC_Connector (args)
    odbc.as_csv ()
# end def main

if __name__ == '__main__' :
    main ()
