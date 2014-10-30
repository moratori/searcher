#!/usr/bin/env python
#coding:utf-8

import socket as s
import pickle
import MySQLdb
import time
import urlparse
import logging
import datetime
import traceback


logging.basicConfig(filename="/home/moratori/Github/searcher/LOG/controller.log")


class Target:
  def __init__(self,d_id,r_id,url):
    self.d_id = d_id
    self.r_id = r_id
    self.url  = url


# Node は Target オブジェクトのコンテナ
class Node:
  def __init__(self):
    self.container = []

  def add(self,target):
    self.container.append(target)

  def get(self):
    return self.container.pop()

  def isempty(self):
    return len(self.container) == 0





class TaskController:

  domain_interval = 40
  resource_interval = 3600 * 24 * 3

  resource_per_domain = 5
  work_load = 25

  def __init__(self, port , backlog , db_host , db_user , db_passwd , db_name):
    self.lsock= s.socket(s.AF_INET , s.SOCK_STREAM , s.IPPROTO_TCP)
    self.lsock.bind(("" , port))
    self.lsock.listen(backlog)

    self.db_connecter = MySQLdb.connect(host=db_host,user=db_user,passwd=db_passwd,db=db_name)
    self.db_cursor    = self.db_connecter.cursor()
  
  def accepter(self):
    while True:
      try:
        (new , (a,p)) = self.lsock.accept()
        print "Accepted: %s:%s" %(a,p)
        self.serv(new)
      except:
        logging.error("\n" + str(datetime.datetime.today()) + "\n" + traceback.format_exc() + "\n")
        break
    self.stop()

  def lookup_dname(self,d_id):
    self.db_cursor.execute("select name from dmapper where d_id = %s" %d_id)
    return self.db_cursor.fetchall()[0][0]


  def __getarget_domain(self):

    """
     ・ホワイトに書いてあるものだけをもってくる(なにもかいてないなら全てもってくる)
     ・ブラックに書いてあるものをもってこない
     ・時間に満たないものはもってこない
    """
    now = int(time.time())

    self.db_cursor.execute("select name from white")
    white_domain = tuple(map(lambda x:x[0],self.db_cursor.fetchall()))

    self.db_cursor.execute("select name from black")
    black_domain = tuple(map(lambda x:x[0],self.db_cursor.fetchall()))
    
    self.db_cursor.execute("select * from dmapper order by vtime asc")
    src_domain = self.db_cursor.fetchall()

    if white_domain:
      src_domain = filter((lambda x: x[1].endswith(white_domain)) , src_domain)
  
    src_domain   = filter((lambda x: (not x[1].endswith(black_domain)) and (now - int(x[2])) > self.domain_interval) , src_domain)
    return list(src_domain)


  def getarget(self):
    """
     (d_id,r_id,URL)のリストを返す
    """

    now = int(time.time())    

    valid_domain = self.__getarget_domain()
    while not valid_domain:
      valid_domain = self.__getarget_domain()
      time.sleep(int(self.domain_interval/2.0))

    ids = map(lambda x: str(x[0]), valid_domain)
      
    self.db_cursor.execute(
        """
        select count(*) from rmapper 
        where ((%s - vtime) > %s) and
        d_id in (%s)
        """ 
        %(now , self.resource_interval , ",".join(ids)))

    ((num,),) = self.db_cursor.fetchall()
    if (num == 0 ) : return []

    now = int(time.time())
    
    result = []
    for (d_id,name,vtime) in valid_domain:
      if len(result) >= self.work_load: break 
      self.db_cursor.execute(
      """
      select * from rmapper
      where 
      ((%s - vtime) > %s) and
      (d_id = %s)
      order by rand()
      limit %s
      """
      %(now , self.resource_interval , d_id , self.resource_per_domain))
      
      node = Node()
      for (r_id , _ , path , vtime , counter) in self.db_cursor.fetchall():
        node.add(Target(d_id,r_id,urlparse.urljoin(name,path))) 
        self.db_cursor.execute("update rmapper set vtime = %s , counter = counter + 1 where r_id = %s" %(now,r_id))

      if not node.isempty(): 
        result.append(node)
        self.db_cursor.execute("update dmapper set vtime = %s where d_id = %s" %(now,d_id))

      self.db_connecter.commit()

    return result


  def serv(self,csock):
    raw = self.getarget()
    data = pickle.dumps(raw)
    header = "Length:%s\n" %len(data)
    csock.sendall(header + data)
    csock.close()

  def stop(self):
    self.lsock.close()
    self.db_cursor.close()
    self.db_connecter.close()




if __name__ == "__main__" :
  (user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())
  TaskController(12345,7,"localhost",user,passwd,"searcher").accepter()



