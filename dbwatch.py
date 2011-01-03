#! /usr/bin/python
# -*- coding: utf-8 -*-
# Copyright  (C) 2011 by Javier Candeira

import os
import time 

import gamin
from opster import command

from dbmap import DBMap


def watch_dirs(inputdirs):
  """Input is a list of directories that hold the data and structure 
  for the database objects.
  
 Output is awesome.
  """
  mon = gamin.WatchMonitor()
  watcheddirs = {}
  mappings = []
  
  for inputdir in inputdirs:
    # establish correspondence between each root watched dir and its db table
    mapping = DBMap(inputdir)
    mappings.append(mapping)
    
    # get all watched dirs (leaf dirs from inputdirs trees) in one place
    tree = os.walk(inputdir)
    subdirs = dict((this,mapping) for this, sub, files in tree if not sub)
    watcheddirs.update(subdirs)
  
  # start the monitoring of all dirs we want watched  
  for subdir, mapping in watcheddirs.items():
    mon.watch_directory(subdir, file_event(subdir, mapping))

  # every second we check for filesystem changes to upload to the db
  while True:
    time.sleep(1)
    mon.handle_events()
    # and if there are such changes, we upload them
    for mapping in mappings:
      if mapping.queue: 
        mapping.update_db() 


def file_event(dirpath, mapping):  
  """Returns the function that updates the database on each event"""
  def queue_file(filepath, event, dirpath=dirpath, mapping=mapping):
    # ignore first events, "exists" etc. and also dotfiles
    # write dirpath as basepath, filepath as file path
    # con su mapping y todo, nano
    fullpath = os.path.join(dirpath, filepath)
    if filepath[0]==".":
      print filepath + " is a dotfile, so ignoring it"
    elif event == 1 or event== 5 and fullpath in mapping.list_all_files():
      if os.path.isdir(fullpath):
        raise Exception("No puedes crear un directorio nuevo en el DBMap")
      mapping.queue.append(fullpath)
    else:        
      # handle other events here: except on dangerous, ignore innocuous ones
      pass # @@TODO
  return queue_file

def check_db(inputdirs):
  """Test whether the filesystem and the database have the same information."""
  for inputdir in inputdirs:
    mapping = DBMap(inputdir)
    mapping.test()
  
def extract_data(inputdirs):
  "Lay out the filesystem structure on the dirs and dump database data there."  
  for inputdir in inputdirs:
    mapping = DBMap(inputdir)
    mapping.extract_records()

opts = [('w', 'watch', True, 'Watch files in directories, update them to the db.'),
        ('t', 'test-only', False, 'Compare the data on the filesystem and the db'),
        ('e', 'extract', False, 'Dump the data from the db to the filesystem'),
        ('d', 'diff', False, 'Diff between the files and the database -- TODO')]

@command(options=opts, usage='%name [OPTIONS] [DIRECTORY ...]')
def main(*directories, **opts):
  """Monitors files on one or more directories and updates their changes 
to a database.

Each directory mapping to a database table is configured in a manifest file.
(See example manifest files for the blocks and templates tables for Slash)."""

  if opts['test_only']:
    directories = [os.path.abspath(d) for d in directories]
    check_db(directories)
  elif opts['extract']:
    extract_data(directories)
  elif opts['diff']:
    pass # @@ TODO
  elif opts['watch']:
    check_db(directories)
    watch_dirs(directories)

if __name__ == "__main__":
  main()
 
