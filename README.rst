======
py-etl
======

ETL: Extract-Transfer-Load
--------------------------

Several scripts here
++++++++++++++++++++

- ldaptest.py: Tool to dump an ldap tree, dump a specific item by dn, or
  compare two trees
- etl.py: Tool load the initial data from the database into the ldap
  directory, also implements the extract-transfer-load tool. The etl.py
  command is called with a subcommand (similar to git). Support sub
  commands are 'etl' (for the normal etl functionality) and 'initial_load'
  to initially load the whole database into LDAP and/or synchronize an
  exisiting LDAP tree with the latest version of the database.
- aes_pkcs7.py is used for password encryption (see below).
- ldaptimestamp.py for generating timestamps of last sync.
- Test drivers as well as test data for regression testing.
- A script for liveness checking: During normal etl run, etl.py updates
  a file ``/tmp/liveness``. The `liveness`` check tests that the file is
  recent enough and returns an appropriate return code (and an optional
  message on standard error).
- In addition some tools to dump out (part of) a database into a csv
  file and anonymisation scripts. These live in the directory
  ``aux-scripts``.

Ecryption of passwords in the database
++++++++++++++++++++++++++++++++++++++

Passwords in the Oracle database are kept unencrypted in clear text. In
LDAP passwords are using a one-way hash function (supported by
OpenLDAP). To synchronize passwords to the Active Directory instances of
the PHs, an additional password attribute ``idnDistributionPassword`` is
kept in LDAP. This is the password from Oracle encrypted with a
symmetric AES key. The encryption uses PKCS7 (RFC 5652) padding with 128
bit (16 byte) blocksize and a random initialization vector (IV)
generated from the Linux hardware random number generator
``/dev/urandom``. Note that ``etl.py`` has a ``-i`` option to set a
fixed IV. This option is used for regression testing, do *not* use this
in production! The passwords are hex-encoded in LDAP.

Configuration variables
+++++++++++++++++++++++

Configuration variables are documented in detail in the corresponding
``docker-etl`` container project.
