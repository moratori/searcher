#!/usr/bin/env python
#coding:utf-8

import socket as s
import pickle
import MySQLdb
import time
import urlparse


class TaskController:

  domain_interval = 30
  resource_interval = 3600 * 24 * 3
  work_load = 10

  def __init__(self, port , backlog , db_host , db_user , db_passwd , db_name):
    self.lsock= s.socket(s.AF_INET , s.SOCK_STREAM , s.IPPROTO_TCP)
    self.lsock.bind(("" , port))
    self.lsock.listen(backlog)

    self.db_connecter = MySQLdb.connect(host=db_host,user=db_user,passwd=db_passwd,db=db_name)
    self.db_cursor    = self.db_connecter.cursor()
  
  def accepter(self):
    while True:
      try:
        (new , addr) = self.lsock.accept()
        self.serv(new)
      except:break
    self.stop()

  def lookup_dname(self,d_id):
    self.db_cursor.execute("select name from dmapper where d_id = %s" %d_id)
    return self.db_cursor.fetchall()[0][0]


  """

  ["d_id","name"] ,\
        DMAPPER ,\
        "as res" ,\
        "where ((%s - vtime) > %s)" %(now,self.d_interval) ,\
        "and ((not exists (select d_id from white limit 1)) or (exists (select d_id from white where res.d_id = white.d_id)))" ,\
        "and (not exists (select d_id from black where res.d_id = black.d_id))" ,\
        "order by rand()" ,\
        "limit %s" %self.max_domain)


    ["r_id","path"] , RMAPPER ,\
          "where" ,\
          "(d_id = %s) and" %d_id ,\
          "((%s - vtime) > %s)" %(now,self.r_interval) ,\
          "order by counter asc" ,\
          "limit %s" %self.max_access)
  """

  def getarget(self):
    now = int(time.time())
    self.db_cursor.execute(
        """
        select * from rmapper as tmp where 
          (tmp.d_id in (select d_id from dmapper where ((%s - vtime) > %s) ))
          and
          ((%s - tmp.vtime) > %s)
          limit %s
        """ %(now , self.domain_interval , now , self.resource_interval , self.work_load))

    result = []
    for (r_id , d_id , path , vtime , counter) in self.db_cursor.fetchall():
      result.append((d_id ,r_id, urlparse.urljoin(self.lookup_dname(d_id) , path)))
      self.db_cursor.execute("update rmapper set vtime = %s where r_id = %s" %(now , r_id))
    self.db_connecter.commit()

    return result


  def serv(self,csock):
    raw = self.getarget()
    data = pickle.dumps(raw)
    header = "Length:%s\n" %len(data)
    csock.send(header + data)
    csock.close()

  def stop(self):
    self.lsock.close()
    self.db_cursor.close()
    self.db_connecter.close()


(user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())


TaskController(
    12345,5,"localhost",user,passwd,"searcher").accepter()


