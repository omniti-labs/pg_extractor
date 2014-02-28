#!/usr/bin/env python
import sys

if sys.version_info[0] != 3:
    print("This script requires Python version 3.0 or greater")
    sys.exit(2)

import argparse, fileinput, os, os.path, re, shutil, socket, subprocess, tempfile

class PGExtractor:

    def __init__(self):
        self.version = "2.0.0beta"
        self.args = False

######################################################################################
#
# PUBLIC METHODS
#
######################################################################################

    ####
    # Build a list of all objects contained in the dump file
    # Currently only returns the following object types:
    ####
    def build_main_object_list(self, restore_file="#default#"):
        main_object_list = []
        if restore_file == "#default#":
            restore_file = self.tmp_dump_file.name
        pg_restore_cmd = ["pg_restore", "--list", restore_file]
        try:
            restore_object_list = subprocess.check_output(pg_restore_cmd, universal_newlines=True).splitlines()
        except subprocess.CalledProcessError as e:
            print("Error in pg_restore when generating main object list: " + str(e.cmd))

        p_objid = '\d+;\s\d+\s\d+'
        # Actual types extracted is controlled in create_extract_files(). This is list format mapping choices
        # Order of this list matters if the string starts with the same word (ex TABLE DATA before TABLE).
        # Last object in list like this cannot have a space in it.
        # If an object type is missing, please let me know and I'll add it.
        p_types = "ACL|AGGREGATE|COMMENT|CONSTRAINT|DATABASE|DEFAULT\sACL|DEFAULT|"
        p_types += "EXTENSION|FK\sCONSTRAINT|FOREIGN\sTABLE|FUNCTION|"
        p_types += "INDEX|RULE|SCHEMA|SEQUENCE\sOWNED\sBY|SEQUENCE\sSET|SEQUENCE|"
        p_types += "TABLE\sDATA|TABLE|TRIGGER|TYPE|VIEW"
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
                r'(?P<objtype>\S+)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objsubtype>\S+)\s'
                r'(?P<objname>\S+)\s')
        p_comment_function_mapping = re.compile(r'(?P<objid>' + p_objid + ')\s'
                r'(?P<objtype>\S+)\s'
                r'(?P<objschema>\S+)\s'
                r'(?P<objsubtype>\S+)\s'
                r'(?P<objname>.*\))\s'
                r'(?P<objowner>\S+)')
        #TODO p_default_acl_mapping = 
        # 8253; 826 31309 DEFAULT ACL affiliate DEFAULT PRIVILEGES FOR SEQUENCES postgres
        # 8254; 826 31310 DEFAULT ACL affiliate DEFAULT PRIVILEGES FOR TABLES postgres
        # objid, objtype, objschema, objstatement, objsubtype, objrole
        # TODO See what these do
        # 11666; 2604 22460 DEFAULT public id web
        if self.args and self.args.debug:
            print("\nPG_RESTORE LIST:")
            for o in restore_object_list:
                print(o)
        for o in restore_object_list:
            if re.match(r'^;', o):
                continue
            obj_type = p_main_object_type.match(o)
            if obj_type != None:
                print(o)
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
                    if re.match(p_objid + r'\s\S+\s\S+\s(FUNCTION|AGGREGATE)', o):
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
                    elif re.match(p_objid + r'\s\S+\s\-\sEXTENSION', o):
                        obj_mapping = p_comment_extension_mapping.match(o)
                        object_dict = dict([('objid', obj_mapping.group('objid'))
                            , ('objtype', obj_mapping.group('objtype'))
                            , ('objschema', obj_mapping.group('objschema'))
                            , ('objsubtype', obj_mapping.group('objsubtype'))
                            , ('objname', obj_mapping.group('objname'))
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

    ####
    # Return a list of objects of a given objtype
    #
    # object_list 
    #   - must be in the format created by build_main_object_list()
    # list_types
    #   - list object of desired object types
    ####
    def build_type_object_list(self, object_list, list_types):
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

    ####
    # Handle directory creation
    ####
    def create_dir(self, dest_dir):
        if os.path.exists(dest_dir) == False:
            try:
                os.mkdir(dest_dir)
            except OSError as e:
                print("Unable to create directory: " + e.filename + ": " + e.strerror)
        return dest_dir
    # end create_dir()

    ####
    # Create extracted DDL files in an organized folder structure
    # Many of the additional folder & filter options are not available when this is called directly
    # pg_dump command uses environment variables for several settings (add list to docstring)
    #
    # object_list
    #   - Must be in the format created by build_main_object_list()
    # target_dir 
    #   - Must be a full directory path.
    #   - Directory will be created if it does not exist.
    #   - Allows direct calls to this function to have a working base directory
    ####
    def create_extract_files(self, object_list, target_dir="#default#"):
        extract_file_list = []
        if target_dir == "#default#":
            # Allows direct calls to this function to be able to have a working base directory
            target_dir = self.args.basedir
        if self.args and self.args.temp != None:
            tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list', dir=self.args.temp)
        else:
            tmp_restore_list = tempfile.NamedTemporaryFile(prefix='pg_extractor_restore_list')

        acl_list = self.build_type_object_list(object_list, ["ACL"])
        comment_list = self.build_type_object_list(object_list, ["COMMENT"])

        # Objects extracted with pg_dump
        for o in self.build_type_object_list(object_list, ["TABLE", "VIEW", "FOREIGN TABLE"]):
            output_file = target_dir
            if self.args and self.args.schemadir:
                if o.get('objschema') != "-":
                    output_file = self.create_dir(os.path.join(output_file, o.get('objschema')))

            if o.get('objtype') == "TABLE" or o.get('objtype') == "FOREIGN TABLE":
                output_file = self.create_dir(os.path.join(output_file, "tables"))
            elif o.get('objtype') == "VIEW":
                output_file = self.create_dir(os.path.join(output_file, "views"))
            else:
                print("Invalid dump type in create_extract_files() module")
                sys.exit(2)

            output_file = os.path.join(output_file, o.get('objschema') + "." + o.get('objname') + ".sql")
            extract_file_list.append(output_file)
            # TODO Do parallel dump stuff here
            # TODO Have it queue up jobs in the run list and when -j number is hit, kick them off and clear the queue
            self._run_pg_dump(o, output_file)

        # Objects that can be overloaded
        func_agg_list = self.build_type_object_list(object_list, ["FUNCTION", "AGGREGATE"])
        dupe_list = func_agg_list
        for o in func_agg_list:
            output_file = target_dir
            if self.args and self.args.schemadir:
                if o.get('objschema') != "-":
                    output_file = self.create_dir(os.path.join(output_file, o.get('objschema')))
            output_file = self.create_dir(os.path.join(output_file, 'functions'))
            output_file = os.path.join(output_file, o.get('objschema') + "." + o.get('objbasename') + ".sql")
            extract_file_list.append(output_file)
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
            # http://stackoverflow.com/questions/1207406/remove-items-from-a-list-while-iterating-in-python
            # Loops through currently iterating list and removes all overloads that match the current basename
            func_agg_list[:] = [f for f in func_agg_list if f.get('objbasename') != o.get('objbasename')]
            fh.close()
            self._run_pg_restore(tmp_restore_list.name, output_file)

        # Sequences are special little snowflakes
        if self.args and self.args.getsequences:
            dupe_list = self.build_type_object_list(object_list, ["SEQUENCE SET", "SEQUENCE OWNED BY"])
            for o in self.build_type_object_list(object_list, ["SEQUENCE"]):
                output_file = target_dir
                if self.args and self.args.schemadir:
                    if o.get('objschema') != "-":
                        output_file = os.path.join(output_file, self.create_dir(o.get('objschema')))
                output_file = self.create_dir(os.path.join(output_file, 'sequences'))
                output_file = os.path.join(output_file, o.get('objschema') + "." + o.get('objname') + ".sql")
                extract_file_list.append(output_file)
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
                    if c.get('objsubtype') == "EXTENSION":
                        continue    # avoid weird extension comment format
                    if o.get('objschema') == c.get('objschema') and o.get('objname') == c.get('objname'):
                        restore_line =  c.get('objid') + " " + c.get('objtype') + " " + c.get('objschema')
                        restore_line += " " + c.get('objname') + " " + c.get('objowner') + "\n"
                        fh.write(restore_line)
                        fh.close()
                self._run_pg_restore(tmp_restore_list.name, output_file)

        # All other objects extracted via _run_pg_restore()
        for o in self.build_type_object_list(object_list, ["RULE", "SCHEMA", "TRIGGER", "TYPE"]):
            output_file = target_dir
            if self.args and self.args.schemadir:
                if o.get('objschema') != "-":
                    output_file = os.path.join(output_file, self.create_dir(o.get('objschema')))

            if o.get('objtype') == "RULE":
                output_file = self.create_dir(os.path.join(output_file, 'rules'))
                output_file = os.path.join(output_file, o.get('objschema') + "." + o.get('objname') + ".sql")

            if o.get('objtype') == "SCHEMA":
                output_file = self.create_dir(os.path.join(output_file, 'schemata'))
                output_file = os.path.join(output_file, o.get('objname') + ".sql")

            if o.get('objtype') == "TRIGGER":
                output_file = self.create_dir(os.path.join(output_file, 'triggers'))
                output_file = os.path.join(output_file, o.get('objschema') + "." + o.get('objname') + ".sql")

            if o.get('objtype') == "TYPE":
                output_file = self.create_dir(os.path.join(output_file, 'types'))
                output_file = os.path.join(output_file, o.get('objschema') + "." + o.get('objname') + ".sql")

            extract_file_list.append(output_file)
            fh = open(tmp_restore_list.name, 'w')
            restore_line =  o.get('objid') + " " + o.get('objtype') + " " + o.get('objschema')
            restore_line += " " + o.get('objname') + " " + o.get('objowner') + "\n"
            fh.write(restore_line)
            for a in acl_list:
                if o.get('objschema') == a.get('objschema') and o.get('objname') == a.get('objname'):
                    restore_line =  a.get('objid') + " " + a.get('objtype') + " " + a.get('objschema')
                    restore_line += " " + a.get('objname') + " " + a.get('objowner') + "\n"
                    fh.write(restore_line)
            for c in comment_list:
                if c.get('objsubtype') == "EXTENSION":
                    continue    # avoid weird extension comment format
                if o.get('objschema') == c.get('objschema') and o.get('objname') == c.get('objname'):
                    restore_line =  c.get('objid') + " " + c.get('objtype') + " " + c.get('objschema')
                    restore_line += " " + c.get('objname') + " " + c.get('objowner') + "\n"
                    fh.write(restore_line)
            fh.close()
            self._run_pg_restore(tmp_restore_list.name, output_file)

        if self.args and self.args.debug:
            print("\nEXTRACT FILE LIST")
            for f in extract_file_list:
               print(f)

        return extract_file_list
    # end create_extract_files()

    ####
    # Delete files that don't exist in a list of given files
    # Delete folders in a given path if they are empty
    # keep_file_list
    #   - list object containing full paths to files that should remain
    # target_dir
    #   - Full path to target directory of files to clean up
    ####
    def delete_files(self, keep_file_list, target_dir="#default#"):
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
            if root != target_dir and len(files) == 0:
                if self.args and self.args.debug:
                    print("DELETE EMPTY DIR: " + root)
                os.rmdir(root)


    ####
    # Extract role ddl
    # TODO: hide_passwords (not yet working)
    #   - Remove the password hash from the output file
    # output_dir 
    #   - Full path to a target directory for the roles file
    ####
    def extract_roles(self, hide_passwords=False, output_dir="#default#"):
        pg_dumpall_cmd = ["pg_dumpall", "--roles-only"]
        if output_dir == "#default#":
            output_file = self.create_dir(os.path.join(self.args.basedir, "roles"))
        else:
            output_file = self.create_dir(output_dir)
        output_file = os.path.join(output_file, "roles.sql")
        pg_dumpall_cmd.append("--file=" + output_file)
        if self.args.debug:
            print("\nEXTRACT ROLE STATEMENT: " + str(pg_dumpall_cmd))
        try:
            subprocess.check_call(pg_dumpall_cmd)
        except subprocess.CalledProcessError as e:
            print("Error in pg_dumpall command while extracting roles: " + str(e.cmd))
            sys.exit(2)
        return output_file


    ####
    # Print out version
    ####
    def print_version(self):
        print(self.version)

    ####
    # Replace CREATE with CREATE OR REPLACE in view & function files in a given target dir
    ####
    def or_replace(self, target_dir_funcs="#default#", target_dir_views="#default#"):
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
                        print(re.sub(r'^CREATE VIEW\b', "CREATE OR REPLACE VIEW", line), end="")


######################################################################################
#
# PRIVATE METHODS
#
######################################################################################

    ####
    # Built a usable list object from a filter argument
    ####
    def _build_filter_list(self, list_type, list_items, list_prefix="#none#"):
        split_list = []
        if list_type == "csv":
            split_list = list_items.split(',')
        elif list_type == "file":
            try:
                fh = open(list_items, 'r')
                for line in fh:
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
            # returns a string prepended to each list item (used by pg_dump/restore commands)
            return (list_prefix + list_prefix.join(split_list)).strip()
    # end _build_filter_list()

    ####
    # Create the temp dump file used for rest of script runtime
    ####
    def _create_temp_dump(self):
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
            pg_dump_cmd.append(_build_filter_list("csv", self.args.schema_include, " --schema="))
        elif self.args.schema_include_file != None:
            pg_dump_cmd.append(_build_filter_list("file", self.args.schema_include_file, " --schema="))
        if self.args.schema_exclude != None:
            if self.args.schema_exclude_file != None:
                print("Cannot set both --schema_exclude & --schema_exclude_file arguments")
                sys.exit(2)
            pg_dump_cmd.append(_build_filter_list("csv", self.args.schema_exclude, " --exclude-schema="))
        elif self.args.schema_exclude_file != None:
            pg_dump_cmd.append(_build_filter_list("file", self.args.schema_exclude_file, " --exclude-schema="))
        # Table include/exclude done in _filter_object_list(). Doing it here excludes all other objects in the dump file.
        if self.args.debug:
            print(pg_dump_cmd)
        try:
            subprocess.check_call(pg_dump_cmd)
        except subprocess.CalledProcessError as e:
            print("Error in pg_dump command while creating template dump file: " + str(e.cmd))
            sys.exit(2)

        if self.args.keep_dump:
            dest_file = os.path.join(self.create_dir(os.path.join(self.args.basedir, "dump")), "pg_extractor_dump.pgr")
            try:
                shutil.copy(self.tmp_dump_file.name, dest_file)
            except IOError as e:
                print("Error during creation of --keep_dump file: " + e.strerror + ": " + e.filename)
    # end _create_temp_dump()

    ####
    # Filter a dictionary list of objects generated from a pg_restore file
    ####
    def _filter_object_list(self, main_object_list):
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
            if (o.get('objtype') == 'VIEW'):
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
            if (o.get('objtype') == 'TYPE'):
                if (self.args.gettypes == False):
                    continue
            if (o.get('objtype') == 'RULE'):
                if (self.args.getrules == False):
                    continue
            if (o.get('objtype') == 'TRIGGER'):
                if (self.args.gettriggers == False):
                    continue

            print("APPENDING " + str(o))
            filtered_list.append(o)

        if self.args.debug:
            print("\nFILTERED OBJECT LIST")
            for o in filtered_list:
                print(o)
        return filtered_list
    # end _filter_object_list()

    ####
    # Parse command line arguments
    ####
    def _parse_arguments(self):
        self.parser = argparse.ArgumentParser(description="A script for doing advanced dump filtering and managing schema for PostgreSQL databases. See NOTES section at the top of the script source for more details and examples.")
        args_conn = self.parser.add_argument_group(title="Database Connection")
        args_conn.add_argument('--host', default=socket.gethostname(), help="Database server host or socket directory used by pg_dump. Different than --hostname option under directory settings. (Default: Result of socket.gethostname())")
        args_conn.add_argument('-p', '--port', default="5432", help="Database server port.")
        args_conn.add_argument('-U', '--username', help="Database user name used by pg_dump.")
        args_conn.add_argument('-d', '--dbname', help="Database name to connect to. Also used as directory name under --basedir. If this or the PGDATABASE environmet variable are not set, object folders will be created at the --basedir level.")
        args_conn.add_argument('--encoding', help="Create the dump files in the specified character set encoding. By default, the dump is created in the database encoding.")
        args_conn.add_argument('--pgpass', help="Full file path to location of .pgpass file if not in default location.")

        args_dir = self.parser.add_argument_group(title="Directories")
        args_dir.add_argument('--basedir', default=os.getcwd(), help="Base directory for ddl export. (Default: directory pg_extractor is run from)")
        args_dir.add_argument('--hostnamedir', help="Optional hostname of the database server used as directory name under --basedir to help with organization.")
        args_dir.add_argument('--schemadir', action="store_true", help="Breakout each schema's content into subdirectories under the database directory (.../database/schema/...)")
        args_dir.add_argument('--rolesdir', help="Name of the directory under database name to place the export file with role data. No impact without the --getroles or --getall option.")
        args_dir.add_argument('--pgbin', help="Full folder path of the required postgresql binaries if not located in $PATH: pg_dump, pg_restore, pg_dumpall.")
        args_dir.add_argument('--temp', help="Full folder path to use as temporary space. Defaults to system designated temporary space.")

        args_filter = self.parser.add_argument_group(title="Filters")
        args_filter.add_argument('--getall', action="store_true", help="Exports all tables, views, functions, types and roles. Shortcut to setting almost all --get* options. Does NOT include data or separate sequence, trigger or rule files (see --getsequences, --gettriggers, --getrules).")
        args_filter.add_argument('--getschemata', action="store_true", help="Export schema ddl.")
        args_filter.add_argument('--gettables', action="store_true", help="Export table ddl (includes foreign tables). Each file includes table's indexes, constraints, sequences, comments, rules, triggers and permissions.")
        args_filter.add_argument('--getviews', action="store_true", help="Export view ddl.")
        args_filter.add_argument('--getfuncs', action="store_true", help="Export function and/or aggregate ddl. Overloaded functions will all be in the same base filename. Custom aggregates are put in a separate folder than regular functions.")
        args_filter.add_argument('--gettypes', action="store_true", help="Export custom types.")
        args_filter.add_argument('--getroles', action="store_true", help="Export all roles in the cluster to a single file. A different folder for this file can be specified by --rolesdir if it needs to be kept out of version control.")
        args_filter.add_argument('--getsequences', action="store_true", help="If you need to export unowned sequences, set this option. Note that this will export both owned and unowned sequences to the separate sequence folder. --gettables or --getall will include any sequence that is owned by a table in that table's output file as well. Current sequence values can only be included in the extracted file if --getdata is set.")
        args_filter.add_argument('--gettriggers', action="store_true", help="If you need to export triggers definitions separately, use this option. This does not export the trigger function, just the CREATE TRIGGER statement. Use --getfuncs to get trigger functions. Note that trigger definitions are also included in their associated object files (tables, views, etc).")
        args_filter.add_argument('--getrules', action="store_true", help="If you need to export rules that are not part of a table (like INSTEAD OF on VIEWS), set this option. It is not currently possible to include these in the referring object files. Note that this will also export rules separately that are in table files.")
        args_filter.add_argument('--getdata', action="store_true", help="Include data in the output files. Format will be plaintext (-Fp) unless -Fc option is explicitly given. Note this option can cause a lot of extra disk space usage while the script is being  run. At minimum make sure you have enough space for 3 full dumps of the database to account for all other options that can be set.")
        args_filter.add_argument('-Fc', '--Fc', action="store_true", help="Output in pg_dump custom format (useful with --getdata). Otherwise, default is always plaintext (-Fp) format.")
        args_filter.add_argument('-n', '--schema_include', help="CSV list of schemas to INCLUDE. Object in only these schemas will be exported.")
        args_filter.add_argument('-nf', '--schema_include_file', help="Path to a file listing schemas to INCLUDE. Each schema goes on its own line. Object in only these schemas will be exported.")
        args_filter.add_argument('-N', '--schema_exclude', help="CSV list of schemas to EXCLUDE. All objects in these schemas will be ignored. If both -n and -N are set, pg_extractor follows the same rules as pg_dump for such a case.")
        args_filter.add_argument('-Nf', '--schema_exclude_file', help="Path to a file listing schemas to EXCLUDE. Each schema goes on its own line. All objects in these schemas will be ignored. If both -nf and -Nf are set, pg_extractor follows the same rules as pg_dump for such a case.")
        args_filter.add_argument('-t', '--table_include', help="CSV list of tables to INCLUDE. Only these tables will be extracted.")
        args_filter.add_argument('-tf', '--table_include_file', help="Path to a file listing tables to INCLUDE. Each table goes on its own line.")
        args_filter.add_argument('-T', '--table_exclude', help="CSV list of tables to EXCLUDE. These tables will be not be extracted.")
        args_filter.add_argument('-Tf', '--table_exclude_file', help="Path to a file listing tables to EXCLUDE. Each table goes on its own line.")
        args_filter.add_argument('-v', '--view_include', help="CSV list of views to INCLUDE. Only these views will be extracted.")
        args_filter.add_argument('-vf', '--view_include_file', help="Path to a file listing views to INCLUDE. Each view goes on its own line.")
        args_filter.add_argument('-V', '--view_exclude', help="CSV list of views to EXCLUDE. These views will be not be extracted.")
        args_filter.add_argument('-Vf', '--view_exclude_file', help="Path to a file listing views to EXCLUDE. Each view goes on its own line.")
        args_filter.add_argument('-pf', '--function_include_file', help="Path to a file listing functions/aggregates to INCLUDE. Each function goes on its own line. Only these functions will be extracted.")
        args_filter.add_argument('-Pf', '--function_exclude_file', help="Path to a file listing functions/aggregates to EXCLUDE. Each function goes on its own line. These functions will not be extracted.")
        args_filter.add_argument('-o', '--owner_include', help="CSV list of object owners to INCLUDE. Only objects owned by these owners will be extracted.")
        args_filter.add_argument('-of', '--owner_include_file', help="Path to a file listing object owners to INCLUDE. Each owner goes on its own line.")
        args_filter.add_argument('-O', '--owner_exclude', help="CSV list of object owners to EXCLUDE. Objects owned by these owners will not be extracted.")
        args_filter.add_argument('-Of', '--owner_exclude_file', help="Path to a file listing object owners to EXCLUDE. Each owner goes on its own line.")
        args_filter.add_argument('-rf', '--regex_include_file', help="Path to a file containing regex patterns of objects to INCLUDE. These must be valid, non-rawstring python regex patterns. Each pattern goes on its own line. Note this will match against all objects (tables, views, functions, etc).")
        args_filter.add_argument('-Rf', '--regex_exclude_file', help="Path to a file containing regex patterns of objects to EXCLUDE. These must be valid, non-rawstring python regex patterns. Each pattern goes on its own line. Note this will match against all objects (tables, views, functions, etc). If both -rf and -Rf are set at the same time, items will be excluded first than any that remain will match against include.")
        args_filter.add_argument('--no_owner', action="store_true", help="Do not add commands to extracted files that set ownership of objects to match the original database.")
        args_filter.add_argument('-x', '--no_acl', '--no_privileges', action="store_true", help="Prevent dumping of access privileges (grant/revoke commands")

        args_vc = self.parser.add_argument_group(title="Version Control")

        args_misc = self.parser.add_argument_group(title="Misc")
        args_misc.add_argument('--delete', action="store_true", help="Use when running again on the same destination directory as previous runs so that objects deleted from the database or items that don't match your filters also have their old files deleted. WARNING: This WILL delete ALL .sql files in the destination folder(s) which don't match your desired output and remove empty directories. Not required when using the --svndel or --gitdel option.")
        args_misc.add_argument('--clean', action="store_true", help="Adds DROP commands to the SQL output of all objects. WARNING: For overloaded function/aggregates, this adds drop commands for all versions to the single output file.")
        args_misc.add_argument('--orreplace', action="store_true", help="Modifies the function and view ddl files to replace CREATE with CREATE OR REPLACE.")
        args_misc.add_argument('--inserts', action="store_true", help="Dump data as INSERT commands (rather than COPY). Only useful with --getdata option.")
        args_misc.add_argument('--column_inserts', '--attribute_inserts', action="store_true", help="Dump data as INSERT commands with explicit column names (INSERT INTO table (column, ...) VALUES ...). Only useful with --getdata option.")
        args_misc.add_argument('--keep_dump', action="store_true", help="""Keep a permanent copy of the pg_dump file used to generate the export files. Will only contain schemas designated by original options and will NOT contain data even if --getdata is set. Note that other items filtered out by pg_extractor (including tables) will still be included in the dump file. File will be put in a folder called "dump" under --basedir. """)
        args_misc.add_argument('-q', '--quiet', action="store_true", help="Suppress all program output.")
        args_misc.add_argument('--version', action="store_true", help="Print the version number of pg_extractor.")
        args_misc.add_argument('--debug', action="store_true", help="Provide additional output to aid in debugging. Please run with this enabled and provide all results when reporting any issues.")

        self.args = self.parser.parse_args()
    # end _parse_arguments()

    ####
    # Run pg_dump command to generate ddl file
    ####
    def _run_pg_dump(self, o, output_file):
        pg_dump_cmd = ["pg_dump", "--file="+output_file]
        pg_dump_cmd.append("--table=" + o.get('objschema') + "." + o.get('objname'))
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
            subprocess.check_call(pg_dump_cmd)
        except subprocess.CalledProcessError as e:
            print("Error in pg_dump command while creating extract file: " + str(e.cmd))
            sys.exit(2)
    # end _run_pg_dump()

    ####
    # Run pg_restore command to generate ddl file
    ####
    def _run_pg_restore(self, list_file, output_file):
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
            subprocess.check_call(restore_cmd)
        except subprocess.CalledProcessError as e:
            print("Error in pg_restore command while creating extract file: " + str(e.cmd))
            sys.exit(2)
    # end _run_pg_restore()

    ####
    # Set config options for rest of script runtime
    ####
    def _set_config(self):
        if self.args.temp == None:
            self.tmp_dump_file = tempfile.NamedTemporaryFile(prefix='pg_extractor')
        else:
            self.tmp_dump_file = tempfile.NamedTemporaryFile(prefix='pg_extractor', dir=self.args.temp)

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
        if self.args.debug:
            print(os.environ)
        if self.args.pgbin != None:
            os.environ["PATH"] = self.args.pgbin + ":" + os.environ["PATH"]

        # Change basedir if these are set
        if self.args.hostnamedir != None: 
            self.args.basedir = os.path.join(self.args.basedir, self.args.hostnamedir)
        if "PGDATABASE" in os.environ:
            self.args.basedir = os.path.join(self.args.basedir, os.environ["PGDATABASE"])
        self.create_dir(self.args.basedir)

        if self.args.getall:
            self.args.getschemata = True;
            self.args.gettables = True;
            self.args.getfuncs = True;
            self.args.getviews = True;
            self.args.gettypes = True;
            self.args.getroles = True;
        elif any([a for a in (self.args.getschemata,self.args.gettables,self.args.getfuncs,self.args.getviews
            ,self.args.gettypes,self.args.getroles,self.args.getsequences,self.args.gettriggers,self.args.getrules)]):
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
    # end _set_config()



    # Account for special characters
    #def handle_special_chars(object_list)
        # return special_char_list

    #def git_commit()

    #def svn commit()

if __name__ == "__main__":
    p = PGExtractor()
    p._parse_arguments()

    if p.args.version == True:
        p.print_version()
        sys.exit(1)

    p._set_config()
    p._create_temp_dump()
    main_object_list = p.build_main_object_list()
    filtered_list = p._filter_object_list(main_object_list)
    extracted_files_list = p.create_extract_files(filtered_list)
    if p.args.getroles == True:
        role_file = p.extract_roles(False)
        extracted_files_list.append(role_file)
    if p.args.delete == True:
        p.delete_files(extracted_files_list)
    if p.args.orreplace == True:
        p.or_replace()

"""
LICENSE AND COPYRIGHT
---------------------

PG Extractor (pg_extractor) is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT licenses.

Copyright (c) 2014 OmniTI, Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
"""
