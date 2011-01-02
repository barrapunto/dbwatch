#! /usr/bin/python
# -*- coding: utf-8 -*-

import os
import time 

import gamin
from opster import command

from slashmap import SlashMap


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
    mapping = SlashMap(inputdir)
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
    # ignore first events, "exists" etc.
    # write dirpath as basepath, filepath as file path
    # con su mapping y todo, nano
    if event == 1:                             # file has been written to for !!!
      fullpath = os.path.join(dirpath, filepath)
      if os.path.isdir(fullpath):
        raise Exception("No puedes crear un directorio nuevo en el slashmap")
      mapping.queue.append(fullpath)
    else:        
      # handle other events here: except on dangerous, ignore innocuous ones
      pass
  return queue_file

def check_db(inputdirs):
  """Test whether the filesystem and the database have the same information."""
  for inputdir in inputdirs:
    mapping = SlashMap(inputdir)
    mapping.test()
  
def extract_data(inputdirs):
  "Lay out the filesystem structure on the dirs and dump database data there."  
  for inputdir in inputdirs:
    mapping = SlashMap(inputdir)
    mapping.extract_records()

opts = [('t', 'testonly', False, 'Compare the data on the filesystem and the db'),
        ('e', 'extract', False, 'Dump the data from the db to the filesystem'),
        ('d', 'daemon', False, 'Run as a daemon (for permanent running)')]

@command(options=opts, usage='%name [OPTIONS] [DIRECTORY ...]')
def main(*directories, **opts):
  """Monitors files on one or more directories and updates their changes 
to a database.

Each directory mapping to a database table is configured in a manifest file.
(See example manifest files for the blocks and templates tables for Slash)."""

  if opts['testonly']:
    check_db(directories)
  elif opts['extract']:
    extract_data(directories)
  elif opts['daemon']:
    pass # TODO
  else:
    check_db(directories)
    watch_dirs(directories)

if __name__ == "__main__":
  main()
 
