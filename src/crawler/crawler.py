#!/usr/bin/env python
#coding:utf-8

import urllib2
import urlparse
import sys
import time
import datetime
import re
from HTMLParser import HTMLParser
from sqlutil import *
from functools import partial
import traceback
import logging


DMAPPER = "dmapper"
RMAPPER = "rmapper"
DATA    = "data"
BLACK   = "black"
WHITE   = "white"

db      = "crawler"
host    = "localhost"

logging.basicConfig(filename="crawler.log")


(user,passwd) = map(lambda x:x.strip(),open(".pwd").readlines())



def every(func,seq):
  for each in seq:
    if not func(each):return False
  return True


class DB(Sqlutil):

  def __init__(self,host,user,passwd):
    Sqlutil.__init__(self,host,user,passwd)

  def lookup_domainname(self,d_id):
    result = self.select("name",DMAPPER,"where (d_id = %s)" %d_id)
    return result[0][0]

  def lookup_domainid(self,r_id):
    result = self.select("d_id" , RMAPPER , "where r_id = %s" %r_id)
    return result[0][0]


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


class Crawler:

  # あるドメインへのアクセスは 最低1/3 時間間隔
  d_interval = 3600 * 1/4
  # 同一リソースへのアクセスは最低 24時間間隔
  r_interval = 3600 * 24
  # あるドメインのリソースへのアクセスは 15個以内
  max_access = 15
  # アクセスするドメインは 50個
  max_domain = 50

  def __init__(self):
    self.db = DB(host,user,passwd)
    self.db.open(db)

  def __nexttarget(self):
    result = []
    now = int(time.time())
    target = self.db.select(\
        ["d_id","name"] ,\
        DMAPPER ,\
        "where (%s - vtime) > %s" %(now,self.d_interval) ,\
        "order by vtime asc" ,\
        "limit %s" %self.max_domain)
    for (d_id , name) in target:
      cand = self.db.select(\
          ["r_id","path"] , RMAPPER ,\
          "where" ,\
          "(d_id = %s) and" %d_id ,\
          "((%s - vtime) > %s)" %(now,self.r_interval) ,\
          "order by counter asc" ,\
          "limit %s" %self.max_access)
      # ここで最後に black list と white list みて制限しろ
      tmp = Node()
      for (r_id,path) in cand:
        tmp.add(Target(d_id,r_id , urlparse.urljoin(name,path)))
      if not tmp.isempty() : result.append(tmp)
    return result

  def polling(self,wait=30):
    roots = self.__nexttarget()
    while not roots:
      time.sleep(wait)
      roots = self.__nexttarget()
    return roots


  def crawl_forever(self):
    roots = self.polling()
    try:
      while True:
        while roots:
          node = roots.pop()
          self.crawl(node)
          # commit の粒度が荒い気がする
          self.db.commit()
        roots.extend(self.polling())
    except:
      logging.error("\n" + str(datetime.datetime.today()) + "\n" + traceback.format_exc() + "\n")
    finally:
      self.finish()
      sys.exit()
 

  def crawl(self,node):
    assert isinstance(node,Node)
    for t in node.container:
      self.stamp(t.d_id,t.r_id)
      self.analyze(t.r_id,t.url)

  def erase(self,r_id):
    # db には html を返すであろうコンテンツしか登録しないけど
    # もしそうでなかった場合のために r_id レコードをもつやつを削除する
    self.db.delete("rmapper","where r_id = %s" %r_id)
    self.db.delute("data" , "where r_id = %s" %r_id)
    return


  def analyze(self,r_id,url):
    connection = urllib2.urlopen(url)
    mtype = connection.info().getheader("Content-Type")
    # content-type が明示されていない若しくは
    # text/html でないなら DBから抹消
    if (not mtype) or (not mtype.startswith("text/html")):
      self.erace(r_id)
      return
    html_raw_data = connection.read()
    
    apply(partial(self.save,r_id),HTMLAnalizer(url,mtype,html_raw_data).start())

  def save(self,r_id,utftitle,utftext,links):
    # 注意スべきは links であり、すでにDBに格納されているものがあったり、
    # 
    pass


  def stamp(self,d_id,r_id):
    self.db.update("dmapper" , [("vtime" , int(time.time()))] , "where d_id = %s" %d_id)
    self.db.execute("update rmapper set vtime = %s , counter = counter + 1 where r_id = %s" %(int(time.time()),r_id))

  def finish(self):
    self.db.close()


# html に含まれる 全てのURLオブジェクトとutf8のテキストを返す
# text エンコーディングが純粋に書いてあればおｋ
# 書いて無ければ shift-jis と仮定しよう
class HTMLAnalizer(HTMLParser):
  
  default_charset = "shift_jis"

  target = "title"
  untarget_tag  = ["script"]
  target_prp   = ["src","href"]

  exclude_extension = \
     set(["jpg","jpeg","png","gif","ico","bmp",\
       "wmv","wma","wma","wav","mp4","mp3","mid","midi","mov","mpg","mpeg","avi",".swf" ,\
       "xls","xlsx","doc","docx","ppt","pptx","pdf",\
       "zip","rar","lzh","gz","z","cab",\
       "css","js","exe","csv"]) 

  exclude_extension = exclude_extension.union(set(["." + each for each in exclude_extension]))


  def __init__(self,url,mtype,rawhtmldata):
    HTMLParser.__init__(self)

    self.mtype         = mtype
    self.url           = url
    self.rawhtml       = rawhtmldata

    self.innertag_list = []
    self.toplevel_tag  = ""
    self.charset       = self.getcharset(self.mtype)

    self.tdata         = ""
    self.rtext         = ""  
    self.rlinks        = []


  def getcharset(self,mtype):
    tmp = mtype.split(";")
    if len(tmp) == 1 : return None
    target = tmp[1].strip()
    if not target.startswith("charset=") : return None
    (_ , charset)=map(lambda x:x.strip(),target.split("="))
    return charset


  def analizeprp(self,stag,attrs):
    # 指定されたリンクプロパティを得る
    for (attr,val) in attrs:
      val = val.strip()
      if (attr in self.target_prp):
        urlobj = urlparse.urlparse(val)
        self.rlinks.append(\
            urlparse.urlparse(urlparse.urljoin(self.url,val)) if (urlobj.scheme == "")\
            else urlobj)
    # meta タグに記載されているかもしれないキャラセットを求めて ...
    if (stag == "meta") and (not self.charset):
      tmp = dict(map(lambda x:(x[0].lower(),x[1].lower()) , attrs))
      if ("http-equiv" in tmp) and (tmp["http-equiv"] == "content-type") and ("content" in tmp):
        r = self.getcharset(tmp["content"])
        self.charset = r if r else self.default_charset
      else:
        self.charset = self.default_charset


  def handle_starttag(self,stag,attrs):
    self.innertag_list.insert(0,stag)
    self.toplevel_tag = stag
    self.analizeprp(stag,attrs)

  def handle_endtag(self,etag):
    if etag in self.innertag_list:
      self.innertag_list.remove(etag)

  def handle_data(self,data):
    if every(lambda x:(x not in self.innertag_list) , self.untarget_tag):
      self.rtext += data
    if self.target in self.innertag_list:
      self.tdata = data


  def start(self):
    self.feed(self.rawhtml)
    charset = self.default_charset if not self.charset else self.charset
    title = self.tdata.decode(charset).encode("utf8")
    text = ""
    for term in self.rtext.decode(charset).encode("utf8").split("\n"):
      text += term.strip() + " "
    return title,text,self.urlfilter(self.rlinks)

  # 明らかにhtmlファイル返さないであろうurlをフィルタする
  def urlfilter(self,links):
    result = []
    for each in links:
      if every(\
          lambda rule:((not each.path.endswith(rule)) and (not urlparse.urlunparse(each).endswith(rule))),\
          self.exclude_extension):
        result.append(each)
    return result


u = "http://www.yahoo.co.jp/"
(title,text,links) = HTMLAnalizer(u,"",urllib2.urlopen(u).read()).start()

print title
print text
print map(urlparse.urlunparse,links)




