#!/usr/bin/env python
#coding:utf-8

import MySQLdb

# MySQL サーバのための薄いラッパー

def convert(obj):
  return ("\"%s\"" %obj) if isinstance(obj,str) else str(obj)


class Sqlutil:
  
  def __init__(self,host,user,passwd):
    self.user = user
    self.passwd = passwd
    self.host   = host

  def open(self,db,charset="utf8"):
    self.connection = \
    MySQLdb.connect(user=self.user,passwd=self.passwd,host=self.host,db=db,charset=charset)
    self.cursor = self.connection.cursor()
    return

  def commit(self):
    self.connection.commit()
    return

  def __execute(self,sql):
    self.cursor.execute(sql)
    return 

  def __getresult(self):
    return self.cursor.fetchall()

  def select(self,columns,table,*other):
    self.__execute("select %s from %s %s;" \
        %((",".join(columns) if isinstance(columns,list) else columns),table," ".join(other)))
    return self.__getresult()

  def insert(self,table,values):
    values = map(convert,values)
    self.__execute("insert into %s values (%s);" %(table,",".join(values)))
    return

  def update(self,table,setter,*other):
    values = ["%s = %s" %(column , convert(value)) for (column , value) in setter]
    self.__execute("update %s set %s %s;" %(table,",".join(values)," ".join(other)))
    return

  def close(self):
    self.commit()
    self.cursor.close()
    self.connection.close()
    return



def sqlutil_sample():
  a  = Sqlutil("host","user","xxx")
  a.open("dbname")
  a.insert("table" , [(1,"name1"),(2,"name2")])
  a.commit()
  print a.select("*","table")
  a.close()



