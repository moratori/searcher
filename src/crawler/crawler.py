#!/usr/bin/env python
#coding:utf-8

import urllib2
import urlparse
import sys
import time
import re
import HTMLParser
from sqlutil import *


DMAPPER = "dmapper"
RMAPPER = "rmapper"
DATA    = "data"
BLACK   = "black"
WHITE   = "white"

db      = "crawler"
host    = "localhost"


(user,passwd) = map(lambda x:x.strip(),open(".pwd").readlines())


class DB(Sqlutil):

  def __init__(self,host,user,passwd):
    Sqlutil.__init__(self,host,user,passwd)

  def lookup_domainname(self,d_id):
    result = self.select("name",DMAPPER,"where (d_id = %s)" %(d_id))
    return result[0][0]

  def lookup_domainid(self,r_id):
    result = self.select("d_id" , RMAPPER , "where r_id = %s" %(r_id))
    return result[0][0]



class Crawler:

  # あるドメインへのアクセスは 最低1/2 時間間隔
  d_interval = 3600 * 1/2
  # 同一リソースへのアクセスは最低 24時間間隔
  r_interval = 3600 * 24
  # あるドメインのリソースへのアクセスは 15個以内
  max_access = 15

  def __init__(self):
    self.db = DB(host,user,passwd)
    self.db.open(db)

  def nexttarget(self):
    result = []
    now = int(time.time())
    target = self.db.select(\
        ["d_id","name"] ,\
        DMAPPER ,\
        "where (%s - vtime) > %s" %(now,self.d_interval))
    for (d_id , name) in target:
      cand = self.db.select(\
          ["r_id","path"] , RMAPPER ,\
          "where" ,\
          "(d_id = %s) and" %(d_id) ,\
          "((%s - vtime) > %s)" %(now,self.r_interval) ,\
          "order by counter asc" ,\
          "limit %s" %(self.max_access))
      # ここで最後に black list とか white list みて制限しろ
      for (r_id,path) in cand:
        result.append((r_id , urlparse.urljoin(name,path)))
    return result

  def crawl(self):
    pass


  def finish(self):self.db.close()


i = Crawler()
print i.nexttarget()
i.finish()



