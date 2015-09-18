#!/usr/bin/env python

import argparse
import errno
import fileinput
import os
import os.path
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from multiprocessing import Process

class PGExtractor:
    """
    A class object for the PG Extractor PostgreSQL dump filter script. 
    Some public methods are available for individual use outside this script's normal function, 
    but many of its advanced features are only available via the command line interface to the script.
    """

    def __init__(self):
        self.version = "2.3.2"
        self.args = False
        self.temp_filelist = []
        self.error_list = []

######################################################################################
#
# PUBLIC METHODS
#
######################################################################################

    def build_main_object_list(self, restore_file="#default#"):
        """
        Build a list of all objects contained in the dump file 

        * restore_file: full path to a custom format (-Fc) pg_dump file 

        Returns a list containing a dictionary object for each line obtained when running pg_restore -l
        """
        main_object_list = []
        if restore_file == "#default#":
            restore_file = self.tmp_dump_file.name
        pg_restore_cmd = ["pg_restore", "--list", restore_file]
        try:
            restore_object_list = subprocess.check_output(pg_restore_cmd, universal_newlines=True).splitlines()
        except subprocess.CalledProcessError as e:
            print("Error in pg_restore when generating main object list: " + str(e.cmd))
            sys.exit(2)

        p_objid = '\d+;\s\d+\s\d+'
        # Actual types extracted is controlled in create_extract_files(). This is list format mapping choices.
        # Order of this list matters if the string starts with the same word (ex TABLE DATA before TABLE).
        # Last object in this list cannot have a space in it.
        # If an object type is missing, please let me know and I'll add it.
        p_types = "ACL|AGGREGATE|COMMENT|CONSTRAINT|DATABASE|DEFAULT\sACL|DEFAULT|"
        p_types += "DOMAIN|EXTENSION|FK\sCONSTRAINT|FOREIGN\sTABLE|FUNCTION|"
        p_types += "INDEX|RULE|SCHEMA|SEQUENCE\sOWNED\sBY|SEQUENCE\sSET|SEQUENCE|"
        p_types += "TABLE\sDATA|TABLE|TRIGGER|TYPE|VIEW|MATERIALIZED\sVIEW\sDATA|MATERIALIZED\sVIEW"
        p_main_object_type = re.compile(p_objid + r'\s(?P<type>' + p_types + ')')
        p_object_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>' + p_types + ')\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objname>\S+)\s'
                r'(?P<objowner>\S+)')
        p_extension_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>' + p_types + ')\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objname>\S+)\s')
        p_function_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>\S+)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objname>.*\))\s'
                r'(?P<objowner>\S+)')
        p_comment_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>\S+)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objsubtype>\S+)\s'
                r'(?P<objname>\S+)\s'
                r'(?P<objowner>\S+)')
        p_comment_extension_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>COMMENT)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objsubtype>\S+)\s'
                r'(?P<objname>\S+)\s')
        p_comment_function_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>COMMENT)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objsubtype>\S+)\s'
                r'(?P<objname>.*\))\s'
                r'(?P<objowner>\S+)')
        p_comment_dash_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>COMMENT)\s'
                r'(?P<objsubtype>\-)\s'
                r'(?P<objname>\S+)\s'
                r'(?P<objowner>\S+)')
        p_default_acl_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>DEFAULT ACL)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objstatement>DEFAULT PRIVILEGES FOR)\s'
                r'(?P<objsubtype>\S+)\s'
                r'(?P<objrole>\S+)')
        if self.args and self.args.debug:
            print("\nPG_RESTORE LIST:")
            for o in restore_object_list:
                print(o)
        for o in restore_object_list:
            if re.match(r'^;', o):
                continue
            obj_type = p_main_object_type.match(o)
            if obj_type != None:
                # Matches function/agg or the ACL for them
                if ( re.match(p_objid + r'\s(FUNCTION|AGGREGATE)', o) 
                        or (obj_type.group('type').strip() == "ACL" and re.search(r'\(.*\)', o)) ):
                    obj_mapping = p_function_mapping.match(o)
                    objname = obj_mapping.group('objname')
                    basename = objname[:objname.find("(")]
                    object_dict = dict([('objid', obj_mapping.group('objid'))
                        , ('objtype', obj_mapping.group('objtype'))
                        , ('objschema', obj_mapping.group('objschema'))
                        , ('objname', obj_mapping.group('objname'))
                        , ('objbasename', basename)
                        , ('objowner', obj_mapping.group('objowner'))
                        ])
                    main_object_list.append(object_dict)
                    continue
                if obj_type.group('type').strip() == "EXTENSION":
                    obj_mapping = p_extension_mapping.match(o)
                    object_dict = dict([('objid', obj_mapping.group('objid'))
                        , ('objtype', obj_mapping.group('objtype'))
                        , ('objschema', obj_mapping.group('objschema'))
                        , ('objname', obj_mapping.group('objname'))
                        ])
                    main_object_list.append(object_dict)
                    continue
                if obj_type.group('type').strip() == "COMMENT":
                    if re.match(p_objid + r'\s\COMMENT\s\S+\s(FUNCTION|AGGREGATE)', o):
                        obj_mapping = p_comment_function_mapping.match(o)
                        objname = obj_mapping.group('objname')
                        basename = objname[:objname.find("(")]
                        object_dict = dict([('objid', obj_mapping.group('objid'))
                            , ('objtype', obj_mapping.group('objtype'))
                            , ('objschema', obj_mapping.group('objschema'))
                            , ('objsubtype', obj_mapping.group('objsubtype'))
                            , ('objname', obj_mapping.group('objname'))
                            , ('objbasename', basename)
                            , ('objowner', obj_mapping.group('objowner'))
                            ])
                        main_object_list.append(object_dict)
                        continue
                    elif re.match(p_objid + r'\s\COMMENT\s\-\sEXTENSION', o):
                        obj_mapping = p_comment_extension_mapping.match(o)
                        object_dict = dict([('objid', obj_mapping.group('objid'))
                            , ('objtype', obj_mapping.group('objtype'))
                            , ('objschema', obj_mapping.group('objschema'))
                            , ('objsubtype', obj_mapping.group('objsubtype'))
                            , ('objname', obj_mapping.group('objname'))
                            ])
                        main_object_list.append(object_dict)
                        continue
                    elif re.match(p_objid + r'\s\COMMENT\s\-\s', o):
                        obj_mapping = p_comment_dash_mapping.match(o)
                        object_dict = dict([('objid', obj_mapping.group('objid'))
                            , ('objtype', obj_mapping.group('objtype'))
                            , ('objsubtype', obj_mapping.group('objsubtype'))
                            , ('objname', obj_mapping.group('objname'))
                            , ('objowner', obj_mapping.group('objowner'))
                            ])
                        main_object_list.append(object_dict)
                        continue
                    else:
                        obj_mapping = p_comment_mapping.match(o)
                        object_dict = dict([('objid', obj_mapping.group('objid'))
                            , ('objtype', obj_mapping.group('objtype'))
                            , ('objschema', obj_mapping.group('objschema'))
                            , ('objsubtype', obj_mapping.group('objsubtype'))
                            , ('objname', obj_mapping.group('objname'))
                            , ('objowner', obj_mapping.group('objowner'))
                            ])
                        main_object_list.append(object_dict)
                        continue
                if obj_type.group('type').strip() == "DEFAULT ACL":
                    obj_mapping = p_default_acl_mapping.match(o)
                    object_dict = dict([('objid', obj_mapping.group('objid'))
                        , ('objtype', obj_mapping.group('objtype'))
                        , ('objschema', obj_mapping.group('objschema'))
                        , ('objstatement', obj_mapping.group('objstatement'))
                        , ('objsubtype', obj_mapping.group('objsubtype'))
                        , ('objrole', obj_mapping.group('objrole'))
                        ])
                    main_object_list.append(object_dict)
                    continue
                # all the other common object formats
                obj_mapping = p_object_mapping.match(o)
                object_dict = dict([('objid', obj_mapping.group('objid'))
                    , ('objtype', obj_mapping.group('objtype'))
                    , ('objschema', obj_mapping.group('objschema'))
                    , ('objname', obj_mapping.group('objname'))
                    , ('objowner', obj_mapping.group('objowner'))
                    ])
                main_object_list.append(object_dict)

        if self.args and self.args.debug:
            print("\nMAIN OBJECT LIST")
            for o in main_object_list:
                print(o)

        return main_object_list
    # end build_main_object_list()


    def build_type_object_list(self, object_list, list_types):
        """
        Build a list of objects only of the given types. 

        * object_list - a list in the format created by build_main_object_list 
        * list_types - a list of desired object types (objtype field in object_list) 

        Returns a filtered list in the same format as object_list
        """
        type_object_list = []
        for o in object_list:
            for t in list_types:
                # Ensure it matches only the exact type given (ex. "SEQUENCE", not "SEQUENCE SET"
                if re.match(r'^' + t + '$', o.get('objtype')):
                    type_object_list.append(o)

        if self.args and self.args.debug:
            print("\nTYPE OBJECT LIST " + str(list_types))
            for o in type_object_list:
                print(o)

        return type_object_list
    # end build_type_object_list()


    def create_dir(self, dest_dir):
        """
        Create the given directory if it does not exist.
        Must be a full path and full directory tree will be created.

        Returns dest_dir if directory creation was successful, or the directory already exists.
        """
        try:
            os.makedirs(dest_dir)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(dest_dir):
                pass
            else:
                print("Unable to create directory: " + e.filename + ": " + e.strerror)
                sys.exit(2)
        return dest_dir
    # end create_dir()


    def create_extract_files(self, object_list, target_dir="#default#"):
        """
        Create extracted DDL files in an organized folder structure. 
        Many of the additional folder & filter options are not available when this is called directly. 
        pg_dump command uses environment variables for several settings (add list to docstring). 

        * object_list - a list in the format created by build_main_object_list 
        * target_dir - full path to a directory to use as output for extracted files.
            Will be created if it doesn't exist.
            Used in same manner as --basedir option to command line version.
        """
        extract_file_list = []
        if target_dir == "#default#":
            # Allows direct calls to this function to be able to have a working base directory
            target_dir = self.args.basedir

        acl_list = self.build_type_object_list(object_list, ["ACL"])
        comment_list = self.build_type_object_list(object_list, ["COMMENT"])
        process_list = []
        process_count = 0

        # Objects extracted with pg_dump
        pgdump_list = self.build_type_object_list(object_list, ["TABLE", "MATERIALIZED VIEW", "VIEW", "FOREIGN TABLE"])
        if len(pgdump_list) > 0 and self.args and not self.args.quiet:
            print("Extracting tables...")
        for o in pgdump_list:
            output_file = target_dir
            if self.args and self.args.schemadir:
                if o.get('objschema') != "-":
                    output_file = self.create_dir(os.path.join(output_file, o.get('objschema')))

            if o.get('objtype') == "TABLE" or o.get('objtype') == "FOREIGN TABLE":
                output_file = self.create_dir(os.path.join(output_file, "tables"))
            elif o.get('objtype') == "VIEW" or o.get('objtype') == "MATERIALIZED VIEW":
                output_file = self.create_dir(os.path.join(output_file, "views"))
            else:
                print("Invalid dump type in create_extract_files() module")
                sys.exit(2)

            # replace any non-alphanumeric characters with ",hexcode,"
            objschema_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objschema'))
            objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
            output_file = os.path.join(output_file, objschema_filename + "." + objname_filename + ".sql")
            extract_file_list.append(output_file)
            if self.args and self.args.jobs > 0:
                p = Process(target=self._run_pg_dump, args=([o, output_file]))
                if self.args and self.args.debug:
                    print("PG_DUMP PROCESS CREATED: " + str(p.name))
                process_list.append(p)
                if (len(process_list) % self.args.jobs) == 0:
                    if self.args and self.args.debug:
                        print("PG_DUMP PROCESS RUN JOB COUNT REACHED: " + str(len(process_list)))
                    for j in process_list:
                        j.start()
                    for j in process_list:
                        j.join()
                    process_list = []
                process_count += 1
            else:
                self._run_pg_dump(o, output_file)
        # If --jobs value was not reached, finish off any that were left in the queue
        if len(process_list) > 0:
            if self.args and self.args.debug:
                print("PG_DUMP PROCESS RUN REMAINING JOBS: " + str(len(process_list)))
            for j in process_list:
                j.start()
            for j in process_list:
                j.join()


        # Objects that can be overloaded
        process_list = []
        process_count = 0
        tmp_restore_list = None
        func_agg_list = self.build_type_object_list(object_list, ["FUNCTION", "AGGREGATE"])
        dupe_list = func_agg_list
        if len(func_agg_list) > 0 and self.args and not self.args.quiet:
            print("Extracting functions & aggregates...")
        for o in func_agg_list:
            output_file = target_dir
            if self.args and self.args.schemadir:
                if o.get('objschema') != "-":
                    output_file = self.create_dir(os.path.join(output_file, o.get('objschema')))
            if o.get('objtype') == "FUNCTION":
                output_file = self.create_dir(os.path.join(output_file, 'functions'))
            elif o.get('objtype') == "AGGREGATE":
                output_file = self.create_dir(os.path.join(output_file, 'aggregates'))
            else:
                print("Invalid object type found while creating function/aggregate extraction files: " + o.get('objtype'))
            # replace any non-alphanumeric characters with ",hexcode,"
            objschema_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objschema'))
            objbasename_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objbasename'))
            output_file = os.path.join(output_file, objschema_filename + "." + objbasename_filename + ".sql")
            extract_file_list.append(output_file)
            if self.args and self.args.temp != None:
                tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', dir=self.args.temp, delete=False)
            else:
                tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', delete=False)
            self.temp_filelist.append(tmp_restore_list.name)
            fh = open(tmp_restore_list.name, 'w')
            # loop over same list to find overloaded functions
            for d in dupe_list:
                if ( o.get('objschema') == d.get('objschema') and 
                        o.get('objbasename') == d.get('objbasename') ):
                    restore_line =  d.get('objid') + " " + d.get('objtype') + " " + d.get('objschema')
                    restore_line += " " + d.get('objname') + " " + d.get('objowner') + "\n"
                    fh.write(restore_line)
            # Should grab all overloaded ACL & COMMENTS since it's matching on basename
            for a in acl_list:
                if "objbasename" in a:
                    if o.get('objschema') == a.get('objschema') and o.get('objbasename') == a.get('objbasename'):
                        restore_line =  a.get('objid') + " " + a.get('objtype') + " " + a.get('objschema') 
                        restore_line += " " + a.get('objname') + " " + a.get('objowner') + "\n"
                        fh.write(restore_line)
            for c in comment_list:
                if re.match(r'(FUNCTION|AGGREGATE)', c.get('objsubtype')):
                    if o.get('objschema') == c.get('objschema') and o.get('objbasename') == c.get('objbasename'):
                        restore_line =  c.get('objid') + " " + c.get('objtype') + " " + c.get('objschema')
                        restore_line += " " + c.get('objname') + " " + c.get('objowner') + "\n"
                        fh.write(restore_line)
            fh.close()
            if self.args and self.args.jobs > 0:
                p = Process(target=self._run_pg_restore, args=([tmp_restore_list.name, output_file]))
                if self.args and self.args.debug:
                    print("PG_RESTORE FUNCTIONS PROCESS CREATED: " + str(p.name))
                process_list.append(p)
                if (len(process_list) % self.args.jobs) == 0:
                    if self.args and self.args.debug:
                        print("PG_RESTORE FUNCTIONS PROCESS RUN JOB COUNT REACHED: " + str(len(process_list)))
                    for j in process_list:
                        j.start()
                    for j in process_list:
                        j.join()
                    process_list = []
                process_count += 1
            else:
                self._run_pg_restore(tmp_restore_list.name, output_file)
        # If --jobs value was not reached, finish off any that were left in the queue
        if len(process_list) > 0:
            if self.args and self.args.debug:
                print("PG_RESTORE FUNCTIONS PROCESS RUN REMAINING JOBS: " + str(len(process_list)))
            for j in process_list:
                j.start()
            for j in process_list:
                j.join()

        # Handle if --orreplace is set with --schemadir. This must be done after view & function files have been exported.
        if self.args.orreplace:
            schema_list = self.build_type_object_list(object_list, ["SCHEMA"])
            for o in schema_list:
                target_dir_funcs = os.path.join(target_dir, o.get('objname'), "functions")
                target_dir_views = os.path.join(target_dir, o.get('objname'), "views")
                self.or_replace(target_dir_funcs, target_dir_views)



        # Sequences are special little snowflakes
        process_list = []
        process_count = 0
        tmp_restore_list = None
        if self.args and self.args.getsequences:
            sequence_list = self.build_type_object_list(object_list, ["SEQUENCE"])
            dupe_list = self.build_type_object_list(object_list, ["SEQUENCE SET", "SEQUENCE OWNED BY"])
            if len(sequence_list) > 0 and self.args and not self.args.quiet:
                print("Extracting sequences...")
            for o in sequence_list:
                output_file = target_dir
                if self.args and self.args.schemadir:
                    if o.get('objschema') != "-":
                        output_file = self.create_dir(os.path.join(output_file, o.get('objschema')))
                output_file = self.create_dir(os.path.join(output_file, 'sequences'))
                # replace any non-alphanumeric characters with ",hexcode,"
                objschema_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objschema'))
                objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
                output_file = os.path.join(output_file, objschema_filename + "." + objname_filename + ".sql")
                extract_file_list.append(output_file)
                if self.args and self.args.temp != None:
                    tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', dir=self.args.temp, delete=False)
                else:
                    tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', delete=False)
                self.temp_filelist.append(tmp_restore_list.name)
                fh = open(tmp_restore_list.name, 'w')
                restore_line =  o.get('objid') + " " + o.get('objtype') + " " + o.get('objschema')
                restore_line += " " + o.get('objname') + " " + o.get('objowner') + "\n"
                fh.write(restore_line)
                for d in dupe_list:
                    if o.get('objschema') == d.get('objschema') and o.get('objname') == d.get('objname'):
                        restore_line =  d.get('objid') + " " + d.get('objtype') + " " + d.get('objschema')
                        restore_line += " " + d.get('objname') + " " + d.get('objowner') + "\n"
                        fh.write(restore_line)
                for a in acl_list:
                    if o.get('objschema') == a.get('objschema') and o.get('objname') == a.get('objname'):
                        restore_line =  a.get('objid') + " " + a.get('objtype') + " " + a.get('objschema')
                        restore_line += " " + a.get('objname') + " " + a.get('objowner') + "\n"
                        fh.write(restore_line)
                for c in comment_list:
                    if re.search(r'SEQUENCE', c.get('objsubtype')):
                        if o.get('objschema') == c.get('objschema') and o.get('objname') == c.get('objname'):
                            restore_line =  c.get('objid') + " " + c.get('objtype') + " " + c.get('objschema')
                            restore_line += " " + c.get('objname') + " " + c.get('objowner') + "\n"
                            fh.write(restore_line)
                fh.close()
                if self.args and self.args.jobs > 0:
                    p = Process(target=self._run_pg_restore, args=([tmp_restore_list.name, output_file]))
                    if self.args and self.args.debug:
                        print("PG_RESTORE SEQUENCE PROCESS CREATED: " + str(p.name))
                    process_list.append(p)
                    if (len(process_list) % self.args.jobs) == 0:
                        if self.args and self.args.debug:
                            print("PG_RESTORE SEQUENCE PROCESS RUN JOB COUNT REACHED: " + str(process_count))
                        for j in process_list:
                            j.start()
                        for j in process_list:
                            j.join()
                        process_list = []
                    process_count += 1
                else:
                    self._run_pg_restore(tmp_restore_list.name, output_file)
            # If --jobs value was not reached, finish off any that were left in the queue
            if len(process_list) > 0:
                if self.args and self.args.debug:
                    print("PG_RESTORE SEQUENCE PROCESS RUN REMAINING JOBS: " + str(len(process_list)))
                for j in process_list:
                    j.start()
                for j in process_list:
                    j.join()


        process_list = []
        process_count = 0
        tmp_restore_list = None
        # Default privileges for roles
        if self.args and self.args.getdefaultprivs:
            acl_default_list = self.build_type_object_list(object_list, ["DEFAULT ACL"])
            dupe_list = acl_default_list
            if len(acl_default_list) > 0 and self.args and not self.args.quiet:
                print("Extracting default privileges...")
            for o in acl_default_list:
                output_file = self.create_dir(os.path.join(target_dir, "roles"))
                output_file = os.path.join(output_file, o.get('objrole') + ".sql")
                extract_file_list.append(output_file)
                if self.args and self.args.temp != None:
                    tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', dir=self.args.temp, delete=False)
                else:
                    tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', delete=False)
                self.temp_filelist.append(tmp_restore_list.name)
                fh = open(tmp_restore_list.name, 'w')
                for d in dupe_list:
                    if o.get('objrole') == d.get('objrole'):
                        restore_line =  d.get('objid') + " " + d.get('objtype') + " " + d.get('objschema')
                        restore_line += " " + d.get('objstatement') + " " + d.get('objrole') + "\n"
                        fh.write(restore_line)
                fh.close()
                if self.args and self.args.jobs > 0:
                    p = Process(target=self._run_pg_restore, args=([tmp_restore_list.name, output_file]))
                    if self.args and self.args.debug:
                        print("PG_RESTORE DEFAULT PRIVS PROCESS CREATED: " + str(p.name))
                    process_list.append(p)
                    if (len(process_list) % self.args.jobs) == 0:
                        if self.args and self.args.debug:
                            print("PG_RESTORE DEFAULT PRIVS PROCESS RUN JOB COUNT REACHED: " + str(len(process_list)))
                        for j in process_list:
                            j.start()
                        for j in process_list:
                            j.join()
                        process_list = []
                    process_count += 1
                else:
                    self._run_pg_restore(tmp_restore_list.name, output_file)
            # If --jobs value was not reached, finish off any that were left in the queue
            if len(process_list) > 0:
                if self.args and self.args.debug:
                    print("PG_RESTORE DEFAULT PRIVS PROCESS RUN REMAINING JOBS: " + str(len(process_list)))
                for j in process_list:
                    j.start()
                for j in process_list:
                    j.join()



        # All other objects extracted via _run_pg_restore()
        process_list = []
        process_count = 0
        tmp_restore_list = None
        other_object_list = self.build_type_object_list(object_list, ["RULE", "SCHEMA", "TRIGGER", "TYPE", "EXTENSION", "DOMAIN"])
        if len(other_object_list) > 0:
            if self.args and not self.args.quiet:
                print("Extracting remaining objects...")
            for o in other_object_list:
                output_file = target_dir
                if self.args and self.args.schemadir:
                    if o.get('objschema') != "-":
                        output_file = self.create_dir(os.path.join(output_file, o.get('objschema')))

                if o.get('objtype') == "RULE":
                    output_file = self.create_dir(os.path.join(output_file, 'rules'))
                    # replace any non-alphanumeric characters with ",hexcode,"
                    objschema_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objschema'))
                    objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
                    output_file = os.path.join(output_file, objschema_filename + "." + objname_filename + ".sql")

                if o.get('objtype') == "SCHEMA":
                    if self.args and self.args.schemadir:
                        output_file = self.create_dir(os.path.join(output_file, o.get('objname')))
                    else:
                        output_file = self.create_dir(os.path.join(output_file, 'schemata'))
                    # replace any non-alphanumeric characters with ",hexcode,"
                    objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
                    output_file = os.path.join(output_file, objname_filename + ".sql")

                if o.get('objtype') == "TRIGGER":
                    output_file = self.create_dir(os.path.join(output_file, 'triggers'))
                    # replace any non-alphanumeric characters with ",hexcode,"
                    objschema_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objschema'))
                    objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
                    output_file = os.path.join(output_file, objschema_filename + "." + objname_filename + ".sql")

                if o.get('objtype') == "TYPE" or o.get('objtype') == "DOMAIN":
                    output_file = self.create_dir(os.path.join(output_file, 'types'))
                    # replace any non-alphanumeric characters with ",hexcode,"
                    objschema_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objschema'))
                    objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
                    output_file = os.path.join(output_file, objschema_filename + "." + objname_filename + ".sql")

                if o.get('objtype') == "EXTENSION":
                    output_file = self.create_dir(os.path.join(output_file, 'extensions'))
                    # replace any non-alphanumeric characters with ",hexcode,"
                    objname_filename = re.sub(r'\W', self.replace_char_with_hex, o.get('objname'))
                    output_file = os.path.join(output_file, objname_filename + ".sql")

                extract_file_list.append(output_file)
                if self.args and self.args.temp != None:
                    tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', dir=self.args.temp, delete=False)
                else:
                    tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', delete=False)
                self.temp_filelist.append(tmp_restore_list.name)
                fh = open(tmp_restore_list.name, 'w')
                restore_line =  o.get('objid') + " " + o.get('objtype') + " " + o.get('objschema')
                if o.get('objtype') == 'EXTENSION':
                    restore_line += " " + o.get('objname') + "\n"
                else:
                    restore_line += " " + o.get('objname') + " " + o.get('objowner') + "\n"
                fh.write(restore_line)
                for a in acl_list:
                    if o.get('objschema') == a.get('objschema') and o.get('objname') == a.get('objname'):
                        restore_line =  a.get('objid') + " " + a.get('objtype') + " " + a.get('objschema')
                        restore_line += " " + a.get('objname') + " " + a.get('objowner') + "\n"
                        fh.write(restore_line)
                for c in comment_list:
                    if re.search(r'(RULE|SCHEMA|TRIGGER|TYPE|EXTENSION|DOMAIN)', c.get('objsubtype')):
                        if o.get('objschema') == c.get('objschema') and o.get('objname') == c.get('objname'):
                            restore_line =  c.get('objid') + " " + c.get('objtype') + " " + c.get('objschema')
                            if c.get('objsubtype') == 'EXTENSION':
                                restore_line += " " + c.get('objname') + "\n"
                            else:
                                restore_line += " " + c.get('objname') + " " + c.get('objowner') + "\n"
                            fh.write(restore_line)
                fh.close()
                if self.args and self.args.jobs > 0:
                    p = Process(target=self._run_pg_restore, args=([tmp_restore_list.name, output_file]))
                    if self.args and self.args.debug:
                        print("PG_RESTORE PROCESS CREATED: " + str(p.name))
                    process_list.append(p)
                    if (len(process_list) % self.args.jobs) == 0:
                        if self.args and self.args.debug:
                            print("PG_RESTORE PROCESS RUN JOB COUNT REACHED: " + str(len(process_list)))
                        for j in process_list:
                            j.start()
                        for j in process_list:
                            j.join()
                        process_list = []
                    process_count += 1
                else:
                    self._run_pg_restore(tmp_restore_list.name, output_file)
            # If --jobs value was not reached, finish off any that were left in the queue
            if len(process_list) > 0:
                if self.args and self.args.debug:
                    print("PG_RESTORE PROCESS RUN REMAINING JOBS: " + str(len(process_list)))
                for j in process_list:
                    j.start()
                for j in process_list:
                    j.join()
        # end if block for other_object_list

        if self.args and self.args.debug:
            print("\nEXTRACT FILE LIST")
            for f in extract_file_list:
               print(f)

        return extract_file_list
    # end create_extract_files()


    def delete_files(self, keep_file_list, target_dir="#default#"):
        """
        Delete files with .sql extension that don't exist in a list of given files. 
        Delete folders in a given path if they are empty. 

        * keep_file_list: list object containing full paths to files that SHOULD REMAIN
        * target_dir: full path to target directory of files to clean up.

        """
        if target_dir == "#default#":
            target_dir = self.args.basedir
        if self.args and self.args.debug:
            print("\nDELETE LIST")
        for root, dirs, files in os.walk(target_dir):
            files = [f for f in files if not f[0] == '.'] # ignore hidden files
            dirs[:] = [d for d in dirs if not d[0] == '.'] # ignore hidden dirs
            for name in files:
                full_file_name = os.path.join(root, name)
                if ( full_file_name not in keep_file_list and
                        re.search(r'\.sql$', name) ):
                    if self.args and self.args.debug:
                        print("DELETE FILE: " + full_file_name)
                    os.remove(full_file_name)

        # Clean up empty folders excluding top root
        for root, dirs, files in os.walk(target_dir):
            files = [f for f in files if not f[0] == '.'] # ignore hidden files
            dirs[:] = [d for d in dirs if not d[0] == '.'] # ignore hidden dirs
            if root != target_dir and len(files) == 0 and len(dirs) == 0:
                if self.args and self.args.debug:
                    print("DELETE EMPTY DIR: " + root)
                os.rmdir(root)
    # end delete_files()


    def extract_roles(self, output_dir="#default#"):
        """
        Extract the roles from the database cluster (uses pg_dumpall -r)

        * output_dir: full path to folder where file will be created. 
            Full directory tree will be created if it does not exist.

        Returns the full path to the output_file that was created.
        """
        pg_dumpall_cmd = ["pg_dumpall", "--roles-only"]
        if (self._check_bin_version("pg_dumpall", "9.0") == True) and (self.args.dbname != None):
            pg_dumpall_cmd.append("--database=" + self.args.dbname)
        if output_dir == "#default#":
            output_file = self.create_dir(os.path.join(self.args.basedir, "roles"))
        else:
            output_file = self.create_dir(output_dir)
        output_file = os.path.join(output_file, "roles.sql")
        pg_dumpall_cmd.append("--file=" + output_file)
        if self.args.debug:
            print("\nEXTRACT ROLE STATEMENT: " + str(pg_dumpall_cmd))
        try:
            subprocess.check_output(pg_dumpall_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print("Error in pg_dumpall command while extracting roles: " + str(e.output, encoding='utf-8').rstrip() + "\nSubprocess command called: " + str(e.cmd))
            sys.exit(2)
        return output_file
    # end extract_roles()


    def print_version(self):
        """ Print out the current version of this script. """
        print(self.version)
    # end print_version()


    def or_replace(self, target_dir_funcs="#default#", target_dir_views="#default#"):
        """
        Replace CREATE with CREATE OR REPLACE in view & function files in a given target dir

        * target_dir_funcs: target directory containing function sql files
        * target_dir_views: target directory containint view sql files
        """
        if target_dir_funcs == "#default#":
            target_dir_funcs = os.path.join(self.args.basedir, "functions")
        if target_dir_views == "#default#":
            target_dir_views = os.path.join(self.args.basedir, "views")
        if self.args and self.args.debug:
            print("\nOR REPLACE LIST")
        if os.path.exists(target_dir_funcs):
            for root, dirs, files in os.walk(target_dir_funcs):
                files = [f for f in files if not f[0] == '.'] # ignore hidden files
                dirs[:] = [d for d in dirs if not d[0] == '.'] # ignore hidden dirs
                for name in files:
                    full_file_name = os.path.join(root, name)
                    if self.args and self.args.debug:
                        print(full_file_name)
                    for line in fileinput.input(full_file_name, inplace=True):
                        print(re.sub(r'^CREATE FUNCTION\b', "CREATE OR REPLACE FUNCTION", line), end="")
        if os.path.exists(target_dir_views):
            for root, dirs, files in os.walk(target_dir_views):
                files = [f for f in files if not f[0] == '.'] # ignore hidden files
                dirs[:] = [d for d in dirs if not d[0] == '.'] # ignore hidden dirs
                for name in files:
                    full_file_name = os.path.join(root, name)
                    if self.args and self.args.debug:
                        print(full_file_name)
                    for line in fileinput.input(full_file_name, inplace=True):
                        # As of V9.4beta2 MATERIALIZED VIEWS cannot use the "CREATE OR REPLACE" syntax
                        print(re.sub(r'^CREATE VIEW\b', "CREATE OR REPLACE VIEW", line), end="")
    # end or_replace()


    def replace_char_with_hex(self, string):
        """
        Replace any non-alphanumeric characters in a given string with their hex values.
        Hex value will be surrounded by commas on either side to distiguish it.

        Example:
                str|ing  ->  str,7c,ng
        """
        return ',{:02x},'.format(ord(string.group()))
    # end replace_char_with_hex()


    def remove_passwords(self, role_file):
        """
        Remove the password hash from a role dump file created by pg_dumpall.
        Leaves the file as valid SQL, but without the PASSWORD parameter to ALTER ROLE.

        * role_file: full path to the dump file
        """
        if os.path.isfile(role_file):
            for line in fileinput.input(role_file, inplace=True):
                if re.match(r'ALTER ROLE', line):
                    print(re.sub(r'(.*)\sPASSWORD\s.*(;)$', r'\1\2', line), end="")
                else:
                    print(line, end="")
        else:
            print("Given role file does not exist: " + role_file)
    # end remove_passwords()

    def show_examples(self):
        print("""
        Basic minimum usage. 
        This will extract all tables, functions/aggregates, views, types & roles. 
        It uses the directory that pg_extractor is run from as the base directory 
        (objects will be found in ./mydb/) and will also produce a permanent copy 
        of the pg_dump file that the objects were extracted from.  It expects the 
        locations of the postgres binaries to be in the $PATH.

            python3 pg_extractor.py -U postgres -d mydb --getall --keep_dump

        Extract only functions from the "keith" schema

            python3 pg_extractor.py -U postgres -d mydb --getfuncs -n keith

        Extract only specifically named functions in the given filename (newline 
        separated list). Ensure the full function signature is given with only 
        the variable types for arguments. Since the functions desired are all 
        in one schema, setting the -n option speeds it up a little since it only 
        has to dump out a single schema to the temp dump file that is used.

            python3 pg_extractor.py -U postgres --dbname=mydb --getfuncs 
                --include_functions_file=/home/postgres/func_incl -n dblink

             func_incl file contains:
             dblink.dblink_exec(text, text)
             dblink.dblink_exec(text, text, boolean)
             dblink.dblink_exec(text)
             dblink.dblink_exec(text, boolean)

        Extract only the tables listed in the given filename list) along 
        with the data in the pg_dump custom format.

            python3 pg_extractor.py -U postgres --dbname=mydb --gettables -Fc 
                -tf /home/postgres/tbl_incl --getdata

        Using an options file

            python3 pg_extractor.py @options_file.txt
        """)


######################################################################################
#
# PRIVATE METHODS
#
######################################################################################

    def _build_filter_list(self, list_type, list_items, list_prefix="#none#"):
        """
        Build a list object based on script filter arguments

        * list_type: Format that the list_items paramter is in ("csv" or "file")
        * list_items: either a csv list of items or a file with line separated items
        * list_prefix: a string that is placed on the front of every item in the result list
            Ex: put "-n " before every item for schema filtering the pg_dump command

        Returns list_items as a list object
        """
        split_list = []
        if list_type == "csv":
            split_list = list_items.split(',')
        elif list_type == "file":
            try:
                fh = open(list_items, 'r')
                for line in fh:
                    if not line.strip().startswith('#'):
                        split_list.append(line.strip())
            except IOError as e:
               print("Cannot access include/exclude file " + list_items + ": " + e.strerror)
               sys.exit(2)
        else:
            print("Bad include/exclude list formatting")
            sys.exit(2)
        if list_prefix == "#none#":
            # returns as an unaltered list object (used by _filter_object_list)
            return split_list
        else:
            # returns a list with the 3rd parameter prepended to each item (used by pg_dump/restore commands)
            return [(list_prefix + x) for x in split_list]
    # end _build_filter_list()

    def _check_bin_version(self, bin_file, min_version):
        """
        Returns true if the major (x.x) version of the given postgres binary is greater than or equal to the one given

        * bin_file: binary postgres file that supports a --version argument (pg_dump, pg_dumpall, pg_restore)
            with the output format: bin_file (PostgreSQL) x.x.x
        * min_version: minimum major version (x.x) that this function will return true for

        Returns true or false
        """
        min_version_list = min_version.split(".")
        min_ver1 = int(min_version_list[0])
        min_ver2 = int(min_version_list[1])
        dump_version = subprocess.check_output([bin_file, '--version'], universal_newlines = True).rstrip()
        version_position = dump_version.index(")") + 1   # add one to remove the space after the paren close
        dump_version_list = dump_version[version_position:].split(".")
        dump_ver1 = int(dump_version_list[0])
        dump_ver2 = int(dump_version_list[1])
        if dump_ver1 < min_ver1:
            return False
        else:
            if dump_ver2 < min_ver2:
                return False
        return True


    def _cleanup_temp_files(self):
        """
        Cleanup temporary files left behind by pg_restore. 
        They are not cleaned up automatically because they must be referenced after 
        the file is closed for writing.
        Processes in the script add to the the global list variable temp_filelist 
        declared in constructor.
        """
        if self.args.debug:
            print("\nCLEANUP TEMP FILES")
        for f in self.temp_filelist:
            if self.args.debug:
                print(f)
            if os.path.exists(f):
                os.remove(f)


    def _create_temp_dump(self):
        """
        Create the temp dump file used for rest of script runtime.
        """
        if not self.args.quiet: 
            print("Creating temp dump file...")
        pg_dump_cmd = ["pg_dump"]
        pg_dump_cmd.append("--format=custom")
        # tmp_dump_file is created during _set_config() so it can be used elsewhere easily
        pg_dump_cmd.append("--file=" + self.tmp_dump_file.name)
        if not self.args.getdata:
            # Some object data is only placed in dump file when data is include (ex: sequence values).
            # So include all data even in temp dump so that can be obtained.
            pg_dump_cmd.append("--schema-only")
        if self.args.no_acl:
            pg_dump_cmd.append("--no-acl")
        if self.args.no_owner:
            pg_dump_cmd.append("--no-owner")
        if self.args.inserts:
            pg_dump_cmd.append("--inserts")
        if self.args.column_inserts:
            pg_dump_cmd.append("--column-inserts")
        if self.args.schema_include != None:
            if self.args.schema_include_file != None:
                print("Cannot set both --schema_include & --schema_include_file arguments")
                sys.exit(2)
            for s in self._build_filter_list("csv", self.args.schema_include, "--schema="):
                pg_dump_cmd.append(s)
        elif self.args.schema_include_file != None:
            for s in self._build_filter_list("file", self.args.schema_include_file, "--schema="):
                pg_dump_cmd.append(s)
        if self.args.schema_exclude != None:
            if self.args.schema_exclude_file != None:
                print("Cannot set both --schema_exclude & --schema_exclude_file arguments")
                sys.exit(2)
            for s in self._build_filter_list("csv", self.args.schema_exclude, "--exclude-schema="):
                pg_dump_cmd.append(s)
        elif self.args.schema_exclude_file != None:
            for s in self._build_filter_list("file", self.args.schema_exclude_file, "--exclude-schema="):
                pg_dump_cmd.append(s)
        # Table include/exclude done in _filter_object_list(). Doing it here excludes all other objects in the dump file.
        if self.args.debug:
            print(pg_dump_cmd)
        try:
            self.tmp_dump_file.close()
            subprocess.check_output(pg_dump_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print("Error in pg_dump command while creating template dump file: " + str(e.output, encoding='utf-8').rstrip() + "\nSubprocess command called: " + str(e.cmd))
            sys.exit(2)
            raise

        if self.args.keep_dump:
            dest_file = os.path.join(self.create_dir(os.path.join(self.args.basedir, "dump")), "pg_extractor_dump.pgr")
            try:
                shutil.copy(self.tmp_dump_file.name, dest_file)
            except IOError as e:
                print("Error during creation of --keep_dump file: " + e.strerror + ": " + e.filename)
                sys.exit(2)
    # end _create_temp_dump()


    def _filter_object_list(self, main_object_list):
        """
        Apply any filter arguments that were given to the main object list generated from a pg_restore file

        * main_object_list: dictionary list generated by build_main_object_list

        Returns a dictionary list in the same format as was input, but with all active filters applied.
        """
        filtered_list = []
        regex_exclude_list = []
        regex_include_list = []
        table_exclude_list = []
        table_include_list = []
        view_exclude_list = []
        view_include_list = []
        func_exclude_list = []
        func_include_list = []
        owner_exclude_list = []
        owner_include_list = []

        if self.args.regex_exclude_file != None:
            regex_exclude_list = self._build_filter_list("file", self.args.regex_exclude_file)
        if self.args.regex_include_file != None:
            regex_include_list = self._build_filter_list("file", self.args.regex_include_file)
        if self.args.table_exclude != None:
            table_exclude_list = self._build_filter_list("csv", self.args.table_exclude)
        if self.args.table_exclude_file != None:
            table_exclude_list = self._build_filter_list("file", self.args.table_exclude_file)
        if self.args.table_include != None:
            table_include_list = self._build_filter_list("csv", self.args.table_include)
        if self.args.table_include_file != None:
            table_include_list = self._build_filter_list("file", self.args.table_include_file)
        if self.args.view_exclude != None:
            view_exclude_list = self._build_filter_list("csv", self.args.view_exclude)
        if self.args.view_exclude_file != None:
            view_exclude_list = self._build_filter_list("file", self.args.view_exclude_file)
        if self.args.view_include != None:
            view_include_list = self._build_filter_list("csv", self.args.view_include)
        if self.args.view_include_file != None:
            view_include_list = self._build_filter_list("file", self.args.view_include_file)
        if self.args.function_exclude_file != None:
            func_exclude_list = self._build_filter_list("file", self.args.function_exclude_file)
        if self.args.function_include_file != None:
            func_include_list = self._build_filter_list("file", self.args.function_include_file)
        if self.args.owner_exclude != None:
            owner_exclude_list = self._build_filter_list("csv", self.args.owner_exclude)
        if self.args.owner_exclude_file != None:
            owner_exclude_list = self._build_filter_list("file", self.args.owner_exclude_file)
        if self.args.owner_include != None:
            owner_include_list = self._build_filter_list("csv", self.args.owner_include)
        if self.args.owner_include_file != None:
            owner_include_list = self._build_filter_list("file", self.args.owner_include_file)

        for o in main_object_list:
            # Allow multiple regex lines to be matched against. Exclude then Include
            if o.get('objname') != None:
                regex_continue = False
                for regex in regex_exclude_list:
                    pattern = re.compile(regex)
                    if pattern.search(o.get('objname')) != None:
                        regex_continue = True
                        break
                    regex_continue = False
                for regex in regex_include_list:
                    pattern = re.compile(regex)
                    if pattern.search(o.get('objname')) != None:
                        regex_continue = False
                        break
                    regex_continue = True
                if regex_continue:
                    continue

            if ( o.get('objowner') in owner_exclude_list ):
                continue
            if ( len(owner_include_list) > 0 and o.get('objowner') not in owner_include_list):
                continue
            if (re.match(r'(TABLE|FOREIGN\sTABLE)', o.get('objtype'))):
                if ( self.args.gettables == False or
                        (o.get('objschema') + "." + o.get('objname')) in table_exclude_list ):
                    continue
                if ( len(table_include_list) > 0 and
                        (o.get('objschema') + "." + o.get('objname')) not in table_include_list ):
                    continue
            if (re.match(r'(VIEW|MATERIALIZED\sVIEW)', o.get('objtype'))):
                if ( self.args.getviews == False or
                        (o.get('objschema') + "." + o.get('objname')) in view_exclude_list):
                    continue
                if ( len(view_include_list) > 0 and
                        (o.get('objschema') + "." + o.get('objname')) not in view_include_list ):
                    continue
            if (re.match(r'FUNCTION|AGGREGATE', o.get('objtype'))):
                if ( self.args.getfuncs == False or
                        (o.get('objschema') + "." + o.get('objname')) in func_exclude_list):
                    continue
                if ( len(func_include_list) > 0 and
                        (o.get('objschema') + "." + o.get('objname')) not in func_include_list):
                    continue
            if (o.get('objtype') == 'SCHEMA'):
                if(self.args.getschemata == False):
                    continue
            if (o.get('objtype') == 'TYPE|DOMAIN'):
                if (self.args.gettypes == False):
                    continue
            if (o.get('objtype') == 'RULE'):
                if (self.args.getrules == False):
                    continue
            if (o.get('objtype') == 'TRIGGER'):
                if (self.args.gettriggers == False):
                    continue
            if (o.get('objtype') == 'EXTENSION'):
                if (self.args.getextensions == False):
                    continue

            filtered_list.append(o)

        if self.args.debug:
            print("\nFILTERED OBJECT LIST")
            for o in filtered_list:
                print(o)
        return filtered_list
    # end _filter_object_list()


    def _parse_arguments(self):
        """
        Parse command line arguments. 
        Sets self.args parameter for use throughout class/script.
        """
        self.parser = argparse.ArgumentParser(description="A script for doing advanced dump filtering and managing schema for PostgreSQL databases. See NOTES section at the top of the script source for more details and examples.", epilog="NOTE: You can pass arguments via a file by passing the filename prefixed with an @ (instead of dashes). Each argument must be on its own line and its recommended to use the double-dash (--) options to make the formatting easiest. Ex: @argsfile.txt", fromfile_prefix_chars="@")
        args_conn = self.parser.add_argument_group(title="Database Connection")
        args_conn.add_argument('--host', help="Database server host or socket directory used by pg_dump. Can also be set with PGHOST environment variable. Leaving this unset will allow pg_dump & pg_dumpall to use the default socket connection.)")
        args_conn.add_argument('-p', '--port', default="5432", help="Database server port. Can also set with the PGPORT environment variable.")
        args_conn.add_argument('-U', '--username', help="Database user name used by pg_dump. Can also be set with PGUSER environment variable. Defaults to system username.")
        args_conn.add_argument('-d', '--dbname', help="Database name to connect to. Also used as directory name under --basedir. Can also be set with PGDATABASE environment variable. If this or PGDATABASE are not set, object folders will be created at the --basedir level. Also used for --database(-l) option to pg_dumpall if pg_dumpall version is 9.0+ and dumping role data. Note that pg_dumpall does not recognize PGDATABASE. If pg_dumpall is less than 9.0, the old defaults are used (see PostgreSQL docs for defaults).")
        args_conn.add_argument('--service', help="Defined service to use to connect to a database. Can also be set with the PGSERVICE environment variable.")
        args_conn.add_argument('--encoding', help="Create the dump files in the specified character set encoding. By default, the dump is created in the database encoding. Can also be set with the PGCLIENTENCODING environment variable.")
        args_conn.add_argument('--pgpass', help="Full file path to location of .pgpass file if not in default location. Can also be set with the PGPASSFILE environment variable.")

        args_dir = self.parser.add_argument_group(title="Directories")
        args_dir.add_argument('--basedir', default=os.getcwd(), help="Base directory for ddl export. (Default: directory pg_extractor is run from)")
        args_dir.add_argument('--hostnamedir', help="Optional hostname of the database server used as directory name under --basedir to help with organization.")
        args_dir.add_argument('--schemadir', action="store_true", help="Breakout each schema's content into subdirectories under the database directory (.../database/schema/...)")
        args_dir.add_argument('--dbnamedir', help="By default, a directory is created with the name of the database being dumped to contain everything else. Set this if you want to change the name.")
        args_dir.add_argument('--nodbnamedir', action="store_true", help="Set this option if you do not want a directory with the database name to be created and used. All files/folders will then be created at either the --basedir or --hostnamedir level.")
        args_dir.add_argument('--pgbin', help="Full folder path of the required postgresql binaries if not located in $PATH: pg_dump, pg_restore, pg_dumpall.")
        args_dir.add_argument('--temp', help="Full folder path to use as temporary space. Defaults to system designated temporary space. Note that if you use --getdata, there must be enough temp space for a full, binary dump of the database in the temp location.")

        args_filter = self.parser.add_argument_group(title="Filters", description="All object names given in any filter MUST be fully schema qualified.")
        args_filter.add_argument('--getall', action="store_true", help="Exports all tables, views, functions, types, extensions and roles. Shortcut to setting almost all --get* options. Does NOT include data or separate sequence, trigger or rule files (see --getsequences, --gettriggers, --getrules).")
        args_filter.add_argument('--getschemata', action="store_true", help="Export schema ddl. Included in --getall.")
        args_filter.add_argument('--gettables', action="store_true", help="Export table ddl (includes foreign tables). Each file includes table's indexes, constraints, sequences, comments, rules, triggers. Included in --getall.")
        args_filter.add_argument('--getviews', action="store_true", help="Export view ddl (includes materialized views). Each file includes all rules & triggers. Included in --getall.")
        args_filter.add_argument('--getfuncs', action="store_true", help="Export function and/or aggregate ddl. Overloaded functions will all be in the same base filename. Custom aggregates are put in a separate folder than regular functions. Included in --getall.")
        args_filter.add_argument('--gettypes', action="store_true", help="Export custom types and domains. Included in --getall.")
        args_filter.add_argument('--getextensions', action="store_true", help="Export extensions. Included in --getall. Note this only places a 'CREATE EXTENSION...' line in the file along with any associated COMMENTs. Extension source code is never dumped out with pg_dump. See PostgreSQL docs on extensions for more info.")
        args_filter.add_argument('--getroles', action="store_true", help="Export all roles in the cluster to a single file. A different folder for this file can be specified by --rolesdir if it needs to be kept out of version control. Included in --getall.")
        args_filter.add_argument('--getdefaultprivs', action="store_true", help="Export all the default privilges for roles if they have been set. See the ALTER DEFAULT PRIVILEGES statement for how these are set. Theese are extracted to the same 'roles' folder that --getroles uses. Included in --getall.")
        args_filter.add_argument('--getsequences', action="store_true", help="If you need to export unowned sequences, set this option. Note that this will export both owned and unowned sequences to the separate sequence folder. --gettables or --getall will include any sequence that is owned by a table in that table's output file as well. Current sequence values can only be included in the extracted file if --getdata is set.")
        args_filter.add_argument('--gettriggers', action="store_true", help="If you need to export triggers definitions separately, use this option. This does not export the trigger function, just the CREATE TRIGGER statement. Use --getfuncs to get trigger functions. Note that trigger definitions are also included in their associated object files (tables, views, etc).")
        args_filter.add_argument('--getrules', action="store_true", help="If you need to export rules separately, set this option. Note that rules will also still be included in their associated object files (tables, views, etc).")
        args_filter.add_argument('--getdata', action="store_true", help="Include data in the output files. Format will be plaintext (-Fp) unless -Fc option is explicitly given. Note this option can cause a lot of extra disk space usage while the script is being run. At minimum make sure you have enough space for 3 full dumps of the database to account for all other options that can be set. See note in --temp option for use of temporary disk space when this option is used.")
        args_filter.add_argument('-Fc', '--Fc', action="store_true", help="Output in pg_dump custom format. Only applies to tables and views. Otherwise, default is always plaintext (-Fp) format.")
        args_filter.add_argument('-n', '--schema_include', help="CSV list of schemas to INCLUDE. Object in only these schemas will be exported.")
        args_filter.add_argument('-nf', '--schema_include_file', help="Path to a file listing schemas to INCLUDE. Each schema goes on its own line. Object in only these schemas will be exported. Comments can be precended with #.")
        args_filter.add_argument('-N', '--schema_exclude', help="CSV list of schemas to EXCLUDE. All objects in these schemas will be ignored. If both -n and -N are set, pg_extractor follows the same rules as pg_dump for such a case.")
        args_filter.add_argument('-Nf', '--schema_exclude_file', help="Path to a file listing schemas to EXCLUDE. Each schema goes on its own line. All objects in these schemas will be ignored. If both -nf and -Nf are set, pg_extractor follows the same rules as pg_dump for such a case. Comments can be precended with #.")
        args_filter.add_argument('-t', '--table_include', help="CSV list of tables to INCLUDE. Only these tables will be extracted.")
        args_filter.add_argument('-tf', '--table_include_file', help="Path to a file listing tables to INCLUDE. Each table goes on its own line. Comments can be precended with #.")
        args_filter.add_argument('-T', '--table_exclude', help="CSV list of tables to EXCLUDE. These tables will be not be extracted.")
        args_filter.add_argument('-Tf', '--table_exclude_file', help="Path to a file listing tables to EXCLUDE. Each table goes on its own line. Comments can be precended with #.")
        args_filter.add_argument('-v', '--view_include', help="CSV list of views to INCLUDE. Only these views will be extracted.")
        args_filter.add_argument('-vf', '--view_include_file', help="Path to a file listing views to INCLUDE. Each view goes on its own line. Comments can be precended with #.")
        args_filter.add_argument('-V', '--view_exclude', help="CSV list of views to EXCLUDE. These views will be not be extracted.")
        args_filter.add_argument('-Vf', '--view_exclude_file', help="Path to a file listing views to EXCLUDE. Each view goes on its own line. Comments can be precended with #.")
        args_filter.add_argument('-pf', '--function_include_file', help="Path to a file listing functions/aggregates to INCLUDE. Each function goes on its own line. Only these functions will be extracted. Comments can be precended with #.")
        args_filter.add_argument('-Pf', '--function_exclude_file', help="Path to a file listing functions/aggregates to EXCLUDE. Each function goes on its own line. These functions will not be extracted. Comments can be precended with #.")
        args_filter.add_argument('-o', '--owner_include', help="CSV list of object owners to INCLUDE. Only objects owned by these owners will be extracted.")
        args_filter.add_argument('-of', '--owner_include_file', help="Path to a file listing object owners to INCLUDE. Each owner goes on its own line. Comments can be precended with #.")
        args_filter.add_argument('-O', '--owner_exclude', help="CSV list of object owners to EXCLUDE. Objects owned by these owners will not be extracted.")
        args_filter.add_argument('-Of', '--owner_exclude_file', help="Path to a file listing object owners to EXCLUDE. Each owner goes on its own line. Comments can be precended with #.")
        args_filter.add_argument('-rf', '--regex_include_file', help="Path to a file containing regex patterns of objects to INCLUDE. These must be valid, non-rawstring python regex patterns. Each pattern goes on its own line. Note this will match against all objects (tables, views, functions, etc). Comments can be precended with #.")
        args_filter.add_argument('-Rf', '--regex_exclude_file', help="Path to a file containing regex patterns of objects to EXCLUDE. These must be valid, non-rawstring python regex patterns. Each pattern goes on its own line. Note this will match against all objects (tables, views, functions, etc). If both -rf and -Rf are set at the same time, items will be excluded first than any that remain will match against include. Comments can be precended with #.")
        args_filter.add_argument('--no_owner', action="store_true", help="Do not add commands to extracted files that set ownership of objects to match the original database.")
        args_filter.add_argument('-x', '--no_acl', '--no_privileges', action="store_true", help="Prevent dumping of access privileges (grant/revoke commands")

        args_misc = self.parser.add_argument_group(title="Misc")
        args_misc.add_argument('-j','--jobs', type=int, default=0, help="Allows parallel running extraction jobs. Set this equal to the number of processors you want to use to allow that many jobs to start simultaneously. This uses multiprocessing library, not threading.")
        args_misc.add_argument('--delete', action="store_true", help="Use when running again on the same destination directory as previous runs so that objects deleted from the database or items that don't match your filters also have their old files deleted. WARNING: This WILL delete ALL .sql files in the destination folder(s) which don't match your desired output and remove empty directories. Not required when using the --svndel or --gitdel option.")
        args_misc.add_argument('--clean', action="store_true", help="Adds DROP commands to the SQL output of all objects. WARNING: For overloaded function/aggregates, this adds drop commands for all versions to the single output file.")
        args_misc.add_argument('--orreplace', action="store_true", help="Modifies the function and view ddl files to replace CREATE with CREATE OR REPLACE.")
        args_misc.add_argument('--remove_passwords', action="store_true", help="If roles are extracted (--getall or --getroles), this option will remove any password hashes from the resulting file.")
        args_misc.add_argument('--inserts', action="store_true", help="Dump data as INSERT commands (rather than COPY). Only useful with --getdata option.")
        args_misc.add_argument('--column_inserts', '--attribute_inserts', action="store_true", help="Dump data as INSERT commands with explicit column names (INSERT INTO table (column, ...) VALUES ...). Only useful with --getdata option.")
        args_misc.add_argument('--keep_dump', action="store_true", help="""Keep a permanent copy of the pg_dump file used to generate the export files. Will only contain schemas designated by original options and will NOT contain data even if --getdata is set. Note that other items filtered out by pg_extractor (including tables) will still be included in the dump file. File will be put in a folder called "dump" under --basedir. """)
        args_misc.add_argument('-w','--wait', default=0, type=float, help="Cause the script to pause for a given number of seconds between each object extraction. If --jobs is set, this is the wait time between parallel job batches. If dumping data, this can help to reduce write load.")
        args_misc.add_argument('-q', '--quiet', action="store_true", help="Suppress all program output.")
        args_misc.add_argument('--version', action="store_true", help="Print the version number of pg_extractor.")
        args_misc.add_argument('--examples', action="store_true", help="Print out examples of command line usage.")
        args_misc.add_argument('--debug', action="store_true", help="Provide additional output to aid in debugging. Please run with this enabled and provide all results when reporting any issues.")
        self.args = self.parser.parse_args()
    # end _parse_arguments()

    def _run_pg_dump(self, o, output_file):
        """
        Run pg_dump for a single object obtained from parsing a pg_restore -l list

        * o: a single object in the dictionary format generated by build_main_object_list
        * output_file: target output file that pg_dump writes to
        """
        pg_dump_cmd = ["pg_dump", "--file=" + output_file]
        pg_dump_cmd.append(r'--table="' + o.get('objschema') + r'"."' + o.get('objname') + r'"')

        if self.args and self.args.Fc:
            pg_dump_cmd.append("--format=custom")
        else:
            pg_dump_cmd.append("--format=plain")
        if self.args and not self.args.getdata:
            pg_dump_cmd.append("--schema-only")
        if self.args and self.args.clean:
            pg_dump_cmd.append("--clean") 
        if self.args and self.args.no_acl:
            pg_dump_cmd.append("--no-acl")
        if self.args and self.args.no_owner:
            pg_dump_cmd.append("--no-owner")
        if self.args and self.args.inserts:
            pg_dump_cmd.append("--inserts")
        if self.args and self.args.column_inserts:
            pg_dump_cmd.append("--column-inserts")
        if self.args.debug:
            print("EXTRACT DUMP: " + str(pg_dump_cmd))
        try:
            subprocess.check_output(pg_dump_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print("Error in pg_dump command while creating extract file: " + str(e.output, encoding='utf-8').rstrip() + "\nSubprocess command called: " + str(e.cmd))
            sys.exit(2)
        if self.args.wait > 0:
            time.sleep(self.args.wait)
    # end _run_pg_dump()


    def _run_pg_restore(self, list_file, output_file):
        """
        Run pg_restore using a file that can be fed to it using the -L option. 
        Assumes a temporary dumpfile was create via _create_temp_dump() and uses that

        * list_file: file containing objects obtained from pg_restore -l that will be restored
        * output_file: target output file that pg_restore writes to
        """
        if self.args.debug:
            fh = open(list_file, 'r')
            print("\nRESTORE LIST FILE CONTENTS")
            for l in fh:
                print(l)
        restore_cmd = ["pg_restore"]
        restore_cmd.append("--use-list=" + list_file)
        restore_cmd.append("--file=" + output_file)
        if self.args and self.args.clean:
            restore_cmd.append("--clean")
        restore_cmd.append(self.tmp_dump_file.name)
        if self.args.debug:
            print("EXTRACT RESTORE: " + str(restore_cmd))
        try:
            subprocess.check_output(restore_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print("Error in pg_restore command while creating extract file: " + str(e.output, encoding='utf-8').rstrip() + "\nSubprocess command called: " + str(e.cmd))
            sys.exit(2)
        if self.args.wait > 0:
            time.sleep(self.args.wait)
    # end _run_pg_restore()


    def _set_config(self):
        """
        Set any configuration options needed for the rest of the script to run
        """
        if self.args.temp == None:
            self.tmp_dump_file = tempfile.NamedTemporaryFile(prefix='pg_extractor')
        else:
            self.tmp_dump_file = tempfile.NamedTemporaryFile(prefix='pg_extractor', dir=self.args.temp, delete=False)
        self.temp_filelist.append(self.tmp_dump_file.name)

        if self.args.pgbin != None:
            sys.path.append(self.args.pgbin)
        if self.args.dbname != None:
            os.environ['PGDATABASE'] = self.args.dbname
        if self.args.host != None:
            os.environ['PGHOST'] = self.args.host
        if self.args.port != None:
            os.environ['PGPORT'] = self.args.port
        if self.args.username != None:
            os.environ['PGUSER'] = self.args.username
        if self.args.pgpass != None:
            os.environ['PGPASSFILE'] = self.args.pgpass
        if self.args.encoding != None:
            os.environ['PGCLIENTENCODING'] = self.args.encoding
        if self.args.service != None:
            os.environ['PGSERVICE'] = self.args.service
        if self.args.debug:
            print(os.environ)
        if self.args.pgbin != None:
            os.environ["PATH"] = self.args.pgbin + ":" + os.environ["PATH"]

        # Change basedir if these are set
        if self.args.hostnamedir != None: 
            self.args.basedir = os.path.join(self.args.basedir, self.args.hostnamedir)
        if self.args.nodbnamedir == True:
            pass # Don't add a dbname to new basedir
        elif self.args.dbnamedir != None:
            self.args.basedir = os.path.join(self.args.basedir, self.args.dbnamedir)
        elif "PGDATABASE" in os.environ:
            self.args.basedir = os.path.join(self.args.basedir, os.environ["PGDATABASE"])
        self.create_dir(self.args.basedir)

        if self.args.getall:
            self.args.getschemata = True
            self.args.gettables = True
            self.args.getfuncs = True
            self.args.getviews = True
            self.args.gettypes = True
            self.args.getroles = True
            self.args.getdefaultprivs = True
            self.args.getextensions = True
        elif any([a for a in (self.args.getschemata,self.args.gettables,self.args.getfuncs,self.args.getviews,self.args.gettypes
            ,self.args.getroles,self.args.getdefaultprivs,self.args.getsequences,self.args.gettriggers,self.args.getrules,self.args.getextensions)]):
            pass # Do nothing since at least one output option was set
        else:
            print("No extraction options set. Must set --getall or one of the other --get<object> arguments.")
            sys.exit(2);

        if ( (self.args.table_include != None and self.args.table_include_file != None) or
                (self.args.table_exclude != None and self.args.table_exclude_file != None) or
                (self.args.view_include != None and self.args.view_include_file != None) or
                (self.args.view_exclude != None and self.args.view_exclude_file != None) or
                (self.args.owner_include != None and self.args.owner_include_file != None) or
                (self.args.owner_exclude != None and self.args.owner_exclude_file != None) ):
            print("Cannot set both a csv and file filter at the same time for the same object type.")
            sys.exit(2)

        if self.args.remove_passwords:
            if not self.args.getroles:
                print("Cannot set --remove_passwords without setting either --getroles or --getall")
                sys.exit(2)
    # end _set_config()

# end PGExtractor class


if __name__ == "__main__":
    p = PGExtractor()
    p._parse_arguments()

    if p.args.version:
        p.print_version()
        sys.exit(1)

    if p.args.examples:
        p.show_examples()
        sys.exit(1)

    try:
        p._set_config()
        p._create_temp_dump()
        main_object_list = p.build_main_object_list()
        filtered_list = p._filter_object_list(main_object_list)
        extracted_files_list = p.create_extract_files(filtered_list)
        if p.args.getroles:
            role_file = p.extract_roles()
            extracted_files_list.append(role_file)
            if p.args.remove_passwords:
                p.remove_passwords(role_file)
        if p.args.delete:
            p.delete_files(extracted_files_list)
        if p.args.orreplace:
            p.or_replace()

        spline = random.randint(1,10000)
        if spline > 9000 and not p.args.quiet:
            print("Reticulating splines...")

    finally:
        if not p.tmp_dump_file.closed:
            p.tmp_dump_file.close()

        p._cleanup_temp_files()
            
    if not p.args.quiet:
        print("Done")
   
"""
LICENSE AND COPYRIGHT
---------------------

PG Extractor (pg_extractor) is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT licenses.

Copyright (c) 2015 OmniTI, Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
"""
