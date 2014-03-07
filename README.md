PGExtractor is both a command line script and python class that can be used to provide more finely 
detailed filtering options for PostgreSQL's pg_dump program. Each database object is extracted into 
its own file organized into folders by type. This makes having the database schema as a reference 
easier and allows for better checking into version control. By default data is not dumped, but 
is easily done with a single option and can be done as plaintext or pg_dump's custom format. Object 
filtering can be done directly via command line options or fed in with external text files. Regex 
pattern matching is also possible.

See --help & --examples for a full list of available options and how to use the script.

The script only uses pg_dump/all to touch the database. 
pg_restore is only used for generating ddl and does not ever touch the database.

Several of the class methods are public and can be used to inspect a custom format binary dump 
file or apply some of the editing options.

Python 3.3.1 (default, Sep 25 2013, 19:29:01) 
[GCC 4.7.3] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from pg_extractor import PGExtractor
>>> p = PGExtractor()
>>> object_list = p.build_main_object_list("dumpfile.pgr")
>>> table_object_list = p.build_type_object_list(object_list, ['TABLE'])
>>> for t in table_object_list:
...     print(t)
... 
{'objname': 'job_detail_p0', 'objid': '238; 1259 596233', 'objowner': 'keith', 'objtype': 'TABLE', 'objschema': 'jobmon'}
{'objname': 'job_detail_p10', 'objid': '239; 1259 596244', 'objowner': 'keith', 'objtype': 'TABLE', 'objschema': 'jobmon'}
...

Remove the password hashes from an existing "pg_dumpall -r" roles file:
>>> p.remove_passwords("pg_dumpall_roles.sql")
