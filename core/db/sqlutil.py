#!/usr/bin/env python
#coding:utf-8

import MySQLdb
import re

# MySQL サーバのための薄いラッパー

def convert(obj):
  return ("\"%s\"" %obj) if isinstance(obj,str) else str(obj)


DMAPPER = "dmapper"
RMAPPER = "rmapper"
DATA    = "data"
LINKR   = "linkr"
BLACK   = "black"
WHITE   = "white"
FREQ    = "freq"
PLACE    = "place"
PAGERANK = "pagerank"



def path_escape(s):
  pat = re.compile('[\\\\"\'`]')
  return re.sub(pat,"",s)


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

  def execute(self,sql):
    self.cursor.execute(sql)
    return 

  def __getresult(self):
    return self.cursor.fetchall()

  def select(self,columns,table,*other):
    self.execute("select %s from %s %s;" \
        %((",".join(columns) if isinstance(columns,list) else columns),table," ".join(other)))
    return self.__getresult()

  def insert(self,table,values,column=""):
    values = map(convert,values)
    self.execute("insert into %s%s values (%s);" %(table,column,",".join(values)))
    return

  def update(self,table,setter,*other):
    values = ["%s = %s" %(column , convert(value)) for (column , value) in setter]
    self.execute("update %s set %s %s;" %(table,",".join(values)," ".join(other)))
    return

  def delete(self,table,*other):
    self.execute("delete from %s %s" %(table," ".join(other)))
    return 

  def close(self):
    self.commit()
    self.cursor.close()
    self.connection.close()
    return



class DB(Sqlutil):

  def __init__(self,host,user,passwd):
    Sqlutil.__init__(self,host,user,passwd)


  def lookup_url(self,r_id):
    path_result = self.select(["d_id","path"] , "rmapper" , "where r_id = %s" %r_id)
    (d_id,path) = path_result[0]
    domain_result = self.select("name" , "dmapper" , "where d_id = %s" %d_id)
    (name,) = domain_result[0]
    return (name,path)

  def lookup_d_id(self,r_id):
    path_result = self.select("d_id" , "rmapper" , "where r_id = %s" %r_id)
    (d_id,) = path_result[0]
    return d_id


  def revlookup_dname(self,dname):
    result = self.select("d_id" , DMAPPER , "where name = \"%s\"" %dname)
    if not result : return None
    return result[0][0]

  def exists_record(self,table,keyname,keyvalue):
    return self.select("*",table,"where %s = %s" %(keyname,keyvalue))

  def exists_rmapper(self,d_id,cand):
    # d_id(dname) であり cand の何れかの path を持つようなのはすでに
    # rmapper に存在するか?
    assert (d_id)
    # ここで lambda の x をエスケープしなければ
    condition = "where " + ("(d_id = %s) and " %d_id) + "(" +"or".join(map(lambda x: "(path = \"%s\")" %path_escape(x), cand)) + ")"
    return self.select("r_id" , RMAPPER ,condition)

  def erase(self,r_id):
    # db には html を返すであろうコンテンツしか登録しないけど
    # もしそうでなかった場合のために r_id レコードをもつやつを削除する
    self.delete(RMAPPER, "where r_id = %s" %r_id)
    self.delete(DATA   , "where r_id = %s" %r_id)
    self.delete(LINKR ,  "where r_id = %s" %r_id)
    self.delete(FREQ  ,  "where r_id = %s" %(r_id))
    self.delete(PLACE ,  "where r_id = %s" %(r_id))
    self.delete(PAGERANK , "where r_id = %s" %r_id)
    return


  # マルチスレッドにするときにこの辺がだめだから
  # 自動でID増えてくようにしないとだめだ

  def next_d_id(self):
    tmp = self.select("max(d_id)" , DMAPPER)
    if not tmp : return 1
    return tmp[0][0] + 1

  def next_r_id(self):
    tmp = self.select("max(r_id)" , RMAPPER)
    if not tmp : return 1
    return tmp[0][0] + 1

  def next_num(self,r_id):
    tmp = self.select("max(num)" , LINKR , "where r_id = %s" %r_id)
    if not tmp : return 1
    ((r,),) = tmp
    return (r+ 1) if r else 1


