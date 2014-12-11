#!/usr/bin/env python
#coding:utf-8


import socket
import time
import pickle
import traceback
import re
import logging
import math
from searcher.core.db.sqlutil import *


english_stop = set([u"of",u"for",u"is",u"am",u"are",u"the",u"an",u"a",u"as",u"in",u"by",u"on",u"to",u"at",u"it",u"its",u"up"])




def count_noun(noun , title , data , param = 1.25):
  # title も考慮して出現頻度を数える
  tmp = data.count(noun)
  return ((len(noun) * param) if (title.count(noun) > 0) else 0) + (0 if tmp == 0 else math.log(tmp+1,4.5))


def stop_word(noun):
    return (noun in english_stop) or ((len(noun) == 1) and (not (re.match(u"[a-zA-Z0-9]",noun) is None)))



class IndexingAgent:

  def __init__(self,port,host,user,passwd,db):
    """
      portにはこのAgentがリッスンするポートを指定する
      hostはDBの接続先ホストを指定
      user,passwd,dbもDB接続に関するパラメータ

      索引生成依頼がきてからDBのオープンを行った方がいい
    """

    self.port   = port

    self.host   = host
    self.user   = user
    self.passwd = passwd
    self.dbname = db

  def opendb(self):
    self.db = DB(self.host ,self.user ,self.passwd)
    self.db.open(self.dbname)


  def closedb(self):
    self.db.close()


  def registplace(self,n_id,r_id,indexes):
    #self.db.delete("place" , "where (n_id = %s) and (r_id = %s)" %(n_id , r_id))
    """
      語の文書における出現位置を全て記録する
      nounregister.pyのindexingの処理で place テーブルは空にさせられるので
      上の delete を行う必要はない
    """
    for (num,index) in enumerate(indexes):
      self.db.insert("place" , [n_id,r_id,num+1,index])

  def registfreq(self,n_id,r_id,freq):
    if self.db.select("*" , "freq" , "where n_id = %s and r_id = %s" %(n_id , r_id)):
      self.db.update("freq" , [("freq",freq)] , "where (n_id = %s) and (r_id = %s)" %(n_id , r_id))
    else:
      self.db.execute("insert into freq(n_id,r_id,freq) values(%s,%s,%s)" %(n_id , r_id , freq))
  
  def wait(self):
    self.server = socket.socket(socket.AF_INET , socket.SOCK_STREAM , socket.IPPROTO_TCP)
    self.server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    self.server.bind(("",self.port))
    self.server.listen(1)
    
    while True:
      try:
        (con , addr) = self.server.accept()
        self.doindexing(self.receive(con))
      except:
        print traceback.format_exc()
        self.stop()
        break

  def receive(self,con):
    print "WORL RECEIVED!!"
    f = con.makefile()
    (_ , size) = f.readline().split(":")
    data = pickle.loads(f.read(int(size)))
    f.close()
    con.close()
    print "WORK LEN: " , len(data)
    return data


  def doindexing(self,r_ids):
    """
      r_id のリストを受けてそれの索引生成を行う
    """

    print "INDEXING START!!"
    self.opendb()
    print "DB OPENED!"
    try:
      all_noun = self.db.select("*","nmapper")
      print "ALL NOUN LEN " , len(all_noun)
      for r_id in r_ids:
        print "DATA r_id " , r_id 
        ((title,data),) = self.db.select(["title","data"] ,"data","where r_id = %s" %r_id )

        tf_avg  = 0
        summing = 0
        cnt     = 0
        cache = {}
        for (n_id,noun) in all_noun:
          if stop_word(noun) : continue
          val = count_noun(noun,title,data)
          if ( val > 0 ):
            cache[(n_id,noun)] = val
            cnt += 1
            summing += val
        if ( cnt == 0 ) : 
          """
          cnt が 0 であることはおかしい
          対象とするデータは語の集合であって全ての各語は事前に 
          nmapper テーブルに登録されている。
          cnt が 0 ということは nmapper テーブルからもってきた語の何れも
          dataに含まれていないということだからそれはあり得ない
          """
          continue
        tf_avg = summing / cnt
        for ((n_id , noun) , val) in cache.items():
          tf = val / tf_avg
          self.registfreq(n_id , r_id , tf)
          self.registplace(n_id , r_id , self.getoccurrence(noun,data))
        self.indexed(r_id)
        self.db.commit()
        
    except:
      print traceback.format_exc()
    finally:
      self.closedb()


  def getoccurrence(self,noun,data):
    # noun が data に出現する全ての位置を返す
    start = data.find(noun,0)
    result = []
    while (start != -1):
      result.append(start)
      start = data.find(noun,start+1)
    return result


  def indexed(self,r_id):
    self.db.update("data" , [("new" , 0)] , "where r_id = %s" %(r_id))


  def stop(self):
    self.server.close()
    return


if __name__ == "__main__": 
  logging.basicConfig(filename="/home/moratori/Github/searcher/LOG/indexingagent.log")
  (user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())
  c = IndexingAgent(4322,
      "localhost",user,passwd,"searcher")
  c.wait()









