======
py-etl
======

ETL: Extract-Transfer-Load

Several scripts here:

- ldaptest.py: Tool to dump an ldap tree, dump a specific item by dn, or
  compare two trees
- etl.py: Tool load the initial data from the database into the ldap
  directory, also will implement the extract-transfer-load tool. In
  addition some tools to dump out (part of) a database into a csv file.
  The latter will probably be removed (and kept in odbc.py).
- odbc.py: Tool to dump (part of) database into a csv file
- log: Some notes
