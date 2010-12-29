#! python2.5
# -*- coding: utf-8 -*-

import os
import ConfigParser
try:
  import oursql as sql
except ImportError:
  import MySQLdb as sql
  sql.paramstyle = 'qmark'

class SlashMap(object):
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
    self.database = config.get("slashmap", "DATABASE")
    self.host = config.get("slashmap", "DBHOST")
    self.user = config.get("slashmap", "DBUSER")
    self.password = config.get("slashmap", "DBPASSWORD")
    self.table = config.get("slashmap", "TABLE")
    self.field = config.get("slashmap", "FIELD")
    self.scheme = path_to_values(config.get("slashmap", "SCHEME"))
    self.dir = directory

  def extract_records(self):
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
      payload = data['template']
      dbrecord = [data[label] for label in (self.scheme)]
      self.write_file(dbrecord, payload)
    return

  def write_file(self, dbrecord, payload):
    """Given data from a row off the db, write the corresponding file"""
    filepath = os.join(dbrecord)+".html"
    fullfilepath = os.path.join(self.dir, filepath)
    dirname, filename = os.path.split(fullfilepath)
    try:
      os.makedirs(dirname) # like os.makedir, but creates intermediate dirs
    except OSError:
      pass                 # we don't mind that directories may already exist
    with open(fullfilepath, 'w') as f:
      f.write(payload)
      f.close()
    
  def update_record(files):
    """Given a list of filepaths, write their contents to the database"""
    conn = sql.connect( host = self.host,
                            user = self.user,
                            passwd = self.password,
                            db = self.database)
    cur = conn.cursor()
    
    for filepath in files:
      try:
        f = open(filepath, 'r')
        f.read(html)
        f.close()
      except IOError:
        raise Exception("Ojo, no puedo leer el archivo en cuestión")
      pattern = self.path_to_recordpattern(filepath)
      updatequery = make_sql_query("UPDATE", self.table, self.field, pattern) 
      cur.execute(updatequery, html)
    conn.close()
       
  def path_to_recordpattern(self, path):
    """Given a path, return a pattern -- a series of field/value pairs"""
    values = path_to_values(path)
    if len(values) != len(self.scheme):
      raise Exception('Something wrong with the db2fs mapping')
    return zip(self.scheme, values)

  def make_sql_query(action, table, field, recordpattern):
    """
    Returns a string with a sql query in it, acting upon 'table' and
    column 'field'.

    recordpattern is a sequence of pairs ("column", "value") that will
    be used as a filter (column1 = value AND column2 = value AND ...)
  
    If action is 'UPDATE' then the query will be an UPDATE too, and
    'field' will be set with the mysql interpolation variable "?".

    If action is 'EXTRACT', the query will be a SELECT.
    """ 
    if action == "EXTRACT":
      what = "SELECT `%s` FROM `%s`" % (field, table)
    elif action == "UPDATE":
      what = "UPDATE `%s` SET `%s` = ?" % (table, field)

    where = " AND ".join(["`%s`='%s'" % fvpair for fvpair in recordpattern]) +";"
    return what + " WHERE " + where
      
def path_to_values(filepath):
  """Make sure the filepath scheme is ok, and return a series of values"""
  return filepath[:-6].split('/')
  
  
  
