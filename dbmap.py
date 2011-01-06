#! python2.5
# -*- coding: utf-8 -*-
# Copyright  (C) 2011 by Javier Candeira

from __future__ import with_statement

import os
try:
  from os.path import relpath as filerelpath
except ImportError:
  from myutils import relpath as filerelpath

import ConfigParser

import MySQLdb as sql

class DBMap(object):
  """Maps a directory into the templates or blocks table of a Slash install
     If templates, the file skin/page/name.html maps to the attributes
     with the same name.
     If blocks, the path scheme is bid.html
  """
  def __init__(self, directory):
    config = ConfigParser.ConfigParser()
    manifest = os.path.join(directory, "manifest")
    config.readfp(open(manifest)) 
    # open manifest file, and, I guess, import everything in it
    self.database = config.get("dbmap", "DATABASE")
    self.host = config.get("dbmap", "DBHOST")
    self.user = config.get("dbmap", "DBUSER")
    self.password = config.get("dbmap", "DBPASSWORD")
    self.table = config.get("dbmap", "TABLE")
    self.field = config.get("dbmap", "FIELD")
    self.scheme = path_to_values(config.get("dbmap", "SCHEME"))
    self.excluded = config.get("dbmap", "EXCLUDED").split("\n")
    self.dir = directory
    self.queue = set()

  def extract_records(self, test=False):
    """Write the mapping object's content to the filesystem"""
    conn = sql.connect( host = self.host,
                            user = self.user,
                            passwd = self.password,
                            db = self.database)
    cur = conn.cursor()
    cur.execute("SELECT * FROM `%s`" % self.table) # all rows by default
    # a pause to reflect: all rows by default, careful with big tables!
    # http://stackoverflow.com/questions/337479/how-to-get-a-row-by-row-mysql-resultset-in-python
    rows = cur.fetchall()
    labels = [x[0] for x in cur.description]
    conn.close()
    
    for row in rows:  
      data = dict(zip(labels, row))
      payload = data[self.field]
      dbrecord = [data[label] for label in (self.scheme)]
      if test:
        self.write_file(dbrecord, payload, test=True)
      else:
        self.write_file(dbrecord, payload)

  def write_file(self, dbrecord, payload, test=False):
    """Given data from a row off the db, write the corresponding file"""
    filepath = os.path.join(*dbrecord) + ".html"
    if filepath in self.excluded:
      print "* Not in the dbmap, listed as excluded: %s" % (filepath,)
      return
    fullfilepath = os.path.join(self.dir, filepath)
    if payload is None:
      payload = "NULL"
    if test:
      with open(fullfilepath, 'r') as f:
        error = "Contents of file %s do not match its record" % fullfilepath,
        assert payload == f.read(), error
        print fullfilepath + " matches its db record."
        f.close() 
    else:
      dirname, filename = os.path.split(fullfilepath)
      try:
        os.makedirs(dirname) # like os.makedir, but creates intermediate dirs
      except OSError:
        pass                 # we don't mind that directories may already exist
      with open(fullfilepath, 'w') as f:
        f.write(payload)
        print "DB record written to %s." % (fullfilepath,)
        f.close()
  
  def update_db(self, files=None, test=False):
    """Writes the contents of the queued files to the database"""
    
    if not files:
      files = self.queue
    
    conn = sql.connect( host = self.host,
                            user = self.user,
                            passwd = self.password,
                            db = self.database)
    cur = conn.cursor()
    
    while files:
      # for every file in the list
      filepath = files.pop()
      # we get the content off the file
      try:
        f = open(filepath, 'r')
        filedata = f.read()
        if filedata == "NULL":
          filedata = None
        f.close()
      except IOError:
        raise Exception("Ojo, no puedo leer el archivo %s" % filepath)

      # we get the part of the filepath that maps to a given row in the table
      relpath = filerelpath(filepath, self.dir)        
      if test:
        # we want to compare the row content with the file content
        readquery = self.make_sql_query("EXTRACT", relpath)
        cur.execute(readquery)        
        rows = cur.fetchall()
        
        rowserror = "No record associated with %s." % relpath
        assert len(rows) > 0, rowserror
        rowserror =  "More than one record associated with %s." % relpath
        assert len(rows) < 2, rowserror        
        
        labels = [x[0] for x in cur.description]
        dbdata = dict(zip(labels, rows[0]))[self.field]
        dbdataerror = "Discrepance in the data associated with %s." % relpath
        assert dbdata == filedata, dbdataerror
      else:
        # or we can just update the filecontent to the corresponding table row
        updatequery = self.make_sql_query("UPDATE", relpath)
        cur.execute(updatequery, filedata)
        print "%s: Change detected, data uploaded to db." % (filepath,)
    conn.close()
       
  def path_to_recordpattern(self, relpath):
    """Given a path, return a pattern -- a series of field/value pairs"""
    values = path_to_values(relpath)
    if len(values) != len(self.scheme):
      raise Exception('Something wrong with the db2fs mapping')
    return zip(self.scheme, values)

  def make_sql_query(self, action, relpath):
    """
    Returns a string with a sql query in it, acting upon 'table' and
    column 'field'.

    Relpath is the file's path relative to the table's directory.
      
    If action is 'UPDATE' then the query will be an UPDATE too, and
    'field' will be set with the mysql interpolation variable "?".

    If action is 'EXTRACT', the query will be a SELECT.
    """ 

    field, table = self.field, self.table
    
    # "pattern" is a sequence of pairs ("column", "value") that will
    # be used as a filter (column1 = value AND column2 = value AND ...)
    pattern = self.path_to_recordpattern(relpath)
    
    if action == "EXTRACT":
      what = "SELECT `%s` FROM `%s`" % (field, table)
    elif action == "UPDATE":
      what = "UPDATE `%s` SET `%s` = %s" % (table, field, "%s")

    where = " AND ".join(["`%s`='%s'" % fvpair for fvpair in pattern])
    return what + " WHERE " + where + ";"
    
  def test(self):
    """Compare the data in the filesystem with the data on the database.
    All data on mapped files should be the same as the data on the database.
    Also there should be no files without corresponding db row, or viceversa."""
    # first we iterate over the records
    self.extract_records(test=True)
    # then we iterate over the files
    allfiles = set(self.list_all_files())
    self.update_db(files=allfiles, test=True)
    
  def list_all_files(self):
    """Gets all *legal* mapped files. 
    Excluded files may exist on filesystem from earlier extract. We ignore them.
    Raises exception if illegal files present or requred files not present."""
    allfiles = []
    fullexcluded = [os.path.join(self.dir, f) for f in self.excluded if f]
    for (dirpath, dirnames, filenames) in os.walk(self.dir):
      if "manifest" in filenames:
        assert dirpath == self.dir, "Manifest only allowed on rootdir."
        filenames.remove("manifest")
      if not (filenames or dirnames):
        raise Exception, "You can't have empty leaf directories!"   
      if (filenames and dirnames):                      
        raise Exception, "You can't have files on non-leaf directories!"
      if filenames:
        filenames = [f for f in filenames if not f[0]=="."]
        fullfilenames = [os.path.join(dirpath, f) for f in filenames]
        for f in fullexcluded:
          fullfilenames.remove(f)
        allfiles.extend(fullfilenames)
    return allfiles
    
def path_to_values(filepath):
  """Make sure the filepath scheme is ok, and return a series of values"""
  if filepath[0] == '/':           # also exception if len == 0, that's good!
    filepath = filepath[1:]        # a leading "/" is always wrong, we fix it. 
  schemestring = ".".join(filepath.split('.')[:-1])   # without file extension.
  scheme = schemestring.split('/')
  return scheme
  
