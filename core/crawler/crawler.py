#!/usr/bin/env python
#coding:utf-8

import urllib2
import urlparse
import sys
import time
import datetime
import re
import traceback
import logging
import getcharset as detect
import random
import hashlib

from HTMLParser import HTMLParser
from searcher.core.db.sqlutil import *



DMAPPER = "dmapper"
RMAPPER = "rmapper"
DATA    = "data"
LINKR   = "linkr"
BLACK   = "black"
WHITE   = "white"

db      = "searcher"
host    = "localhost"


logging.basicConfig(filename="/home/moratori/Github/searcher/core/crawler/crawler.log")
(user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())


def every(func,seq):
  for each in seq:
    if not func(each):return False
  return True

def gethash(s):
  i = hashlib.sha256()
  i.update(s)
  return i.hexdigest()

def remove_unnecessary(l):
  result = []
  tmp    = set()
  for each in l:
    if (not each in tmp) and (each.netloc):
      result.append(each)
      tmp.add(each)
  return result

def getdname(link):
  return (link.scheme + "://" + link.netloc).encode("utf-8")

def getpath(link):
  path = link.path
  query = link.query
  result = ((path if query == "" else path + "?" + query).encode("utf-8"))
  return "/" if result == "" else result

def qescape(text):
  return re.sub(re.compile("[\"\'\\\]"),"",text)

def escape(text):
  # utf-8 な文字列しかできない
  return re.sub(re.compile("[!-/:-@[-`{-~]"),"",text)


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



# html に含まれる 全てのURLオブジェクトとutf8のテキストを返す
# text エンコーディングが純粋に書いてあればおｋ
# 書いて無ければ shift-jis と仮定しよう
# HTTPのContent-Type にcharsetの指定がなく、Metaタグに書いてあっただけの場合は
# HTMLAnalizeの全ての処理(様々文字列処理を内部に含む)が終わった後にdecode.encode
# されるので途中の内部の文字列処理(HTMLParserが正規表現つかってなんかやってるっぽいやつ)
# が例外吐くかもしれん
# なので この HTMLAnalizerに掛ける前に、headの中を特別にみたりして
# エンコーディングの問題は解決してからにするのが好ましい
class HTMLAnalizer(HTMLParser):
  

  target = "title"
  untarget_tag  = ["script","style"]
  target_prp   = ["src","href"]

  exclude_extension = \
     set(["jpg","jpeg","png","gif","ico","bmp",\
       "wmv","wma","wma","wav","mp4","mp3","mid","midi","mov","mpg","mpeg","avi",".swf" ,\
       "xls","xlsx","doc","docx","ppt","pptx","pdf",\
       "zip","rar","lzh","gz","z","cab",\
       "css","js","xml","txt","exe","csv"]) 

  exclude_extension = exclude_extension.union(set(["." + each for each in exclude_extension]))


  def __init__(self,url,mtype,rawhtmldata):
    HTMLParser.__init__(self)

    self.charset       = detect.GetCharset.getcharset(mtype)
    # GetCharsetクラスが頑張ってmetaタグの中みて charset を得るけど、絶対合ってるとはいえないけど
    # その場合はどうしようか
    self.charset       = detect.GetCharset(rawhtmldata).start() if not self.charset else self.charset
    self.rawhtml       = rawhtmldata.decode(self.charset)
    self.url           = url

    self.innertag_list = []
    self.toplevel_tag  = ""

    self.tdata         = ""
    self.rtext         = ""  
    self.rlinks        = []


  def analizeprp(self,stag,attrs):
    # 指定されたリンクプロパティを得る
    for (attr,val) in attrs:
      if not val : continue
      val = val.strip()
      if (attr in self.target_prp):
        urlobj = urlparse.urlparse(val)
        self.rlinks.append((urlparse.urlparse(urlparse.urljoin(self.url,val)) if (urlobj.scheme == "") else urlobj))

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

  # 明らかにhtmlファイル返さないであろうurlをフィルタする
  def urlfilter(self,links):
    result = []
    for each in links:
      if every(\
          lambda rule:((not each.path.endswith(rule)) and (not urlparse.urlunparse(each).endswith(rule))),\
          self.exclude_extension):
        result.append(each)
    return result

  def start(self):
    self.feed(self.rawhtml)
    title = self.tdata
    text = ""
    for each in self.rtext.split("\n"):
      tmp = each.strip()
      if tmp != "" : text += tmp + " "
    return title , text , self.urlfilter(self.rlinks)





class Crawler:
  
  # 同一domainにアクセスするのは 180sec ごのinterval
  # これがでかいと広く浅いcrawlになる
  d_interval = 170

  # 同一リソースへのアクセスは最低 24 * 5時間間隔
  r_interval = 3600 * 24 * 5

  # あるドメインのリソースへのアクセスは 20個以内
  max_access = 20

  # アクセスするドメインは先頭70個を選ぶ
  max_domain = 70

  # コンテンツにアクセスするときのwait
  c_interval = 4

  # 接続要求出して待つ時間
  timeout = 6
  
  # User-Agent は IE9
  useragent = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"

  def __init__(self,dfilter=None):
    self.db = DB(host,user,passwd)
    self.db.open(db)
    self.domainfilter = dfilter


  def __nexttarget_domain(self):
    now = int(time.time())
    tmp = self.db.select(\
        ["d_id","name"] ,\
        DMAPPER ,\
        "as res" ,\
        "where ((%s - vtime) > %s)" %(now,self.d_interval) ,\
        "and ((not exists (select d_id from white limit 1)) or (exists (select d_id from white where res.d_id = white.d_id)))" ,\
        "and (not exists (select d_id from black where res.d_id = black.d_id))" ,\
        "order by rand()" ,\
        "limit %s" %self.max_domain)
    return tmp if not self.domainfilter else filter(lambda x:self.domainfilter(x[1]) , tmp)


  def __nexttarget(self):
    result = []
    now = int(time.time())

    target= self.__nexttarget_domain()

    for (d_id , name) in target:
      cand = self.db.select(\
          ["r_id","path"] , RMAPPER ,\
          "where" ,\
          "(d_id = %s) and" %d_id ,\
          "((%s - vtime) > %s)" %(now,self.r_interval) ,\
          "order by counter asc" ,\
          "limit %s" %self.max_access)
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


  def crawl_toplevel(self):
    roots = self.__nexttarget()
    try:
      while roots:
        ## ここの node らへんをマルチスレッドでアクセスする
        node = roots.pop()
        self.crawl(node)
        # commit の粒度が荒い気がする
        self.db.commit()
        roots.extend(self.__nexttarget())
    except:
      logging.error("\n" + str(datetime.datetime.today()) + "\n" + traceback.format_exc() + "\n")
    finally:
      self.finish()
    return 
 

  def crawl(self,node):
    assert isinstance(node,Node)
    # node は全て同じ ドメインだったはずなので適当にwaitをかける
    for t in node.container:
      self.stamp(t.d_id,t.r_id)
      self.analyze(t.r_id,t.d_id,t.url)
      time.sleep(random.randint(1,self.c_interval))

  # 実際に url にアクセスしてDBに保存したりする処理のコントローラ
  def analyze(self,r_id,d_id,url):
    print "Acessing: %s" %url
    try:
      req = urllib2.Request(url,"",{"User-Agent": self.useragent})
      connection = urllib2.urlopen(req,timeout=self.timeout)
      mtype = connection.info().getheader("Content-Type")
      # content-type が明示されていない若しくは
      # text/html でないなら DBから抹消
      if (not mtype) or (not mtype.startswith("text/html")):
        self.db.erase(r_id)
        return
      html_raw_data = connection.read()
    except:
      logging.warning("can't connect url: %s" %url)
      return
   
    try:
      (uni_title,uni_text,links) = HTMLAnalizer(url,mtype,html_raw_data).start()
    except:
      self.db.erase(r_id)
      logging.warning("unparsible contents. erase r_id = %s from DB" %r_id)
      return
    self.save(r_id,d_id,uni_title,uni_text,links)


  def save(self, r_id , d_id , uni_title , uni_text , links):
    # 注意スべきは links であり、すでにDBに格納されているものがあったり、
    # するので重複しないように格納する処理が必要
    # save は dmapper rmapper data の全てのテーブルを書き換え得る
    # ので書くテーブル毎にメソッドを分けてる
    self.__savedata(r_id,uni_title,uni_text)
    self.__savelinks(r_id , d_id , links)


  def __savelinks(self , r_id , d_id , links):

    def get_indexcand(path):
      if   path.endswith("/"):
        return [path,path + "index.html" , path + "index.htm"]
      elif path.endswith("index.html"):
        base = path[:-10]
        return [path,base,base + "index.htm"]
      elif path.endswith("index.htm"):
        base = path[:9]
        return [path,base,base + "index.html"]
      else:
        return [path]


    # dmapper と rmapper と linkr テーブルの変更
    # まず意味を考えずとも文字列的に一致するもの
    # netlocが存在しないものを除去する
    links = remove_unnecessary(links)

    # +++++++ dmapper に登録する +++++++ #
    for link in links:
      # domain 名でlookup して レコードが存在しないならば新しくinsert する
      dname = getdname(link)
      if not self.db.revlookup_dname(dname):
        new_id = self.db.next_d_id()
        d = [new_id , dname , 0]
        self.db.insert(DMAPPER , d)

    # +++++++ rmapper に登録する ++++++ #
    child_r_ids = []
    for link in links: 
      d_id  = self.db.revlookup_dname(getdname(link))
      assert (d_id)
      path = qescape(getpath(link))
      cand = get_indexcand(path)
      already = self.db.exists_rmapper(d_id,cand)
      if not already:
        new_r_id = self.db.next_r_id()
        self.db.insert(RMAPPER , [new_r_id , d_id , path , 0 , 0])
        child_r_ids.append(new_r_id)
      else:
        child_r_ids.append(already[0][0])

    # +++++++ rmapperの登録の際に生じた r_id と child_r_id の関係をlinkrに記録する +++++++
    next_num = self.db.next_num(r_id)
    for child_r_id in child_r_ids:
      self.db.insert(LINKR,[r_id,next_num,child_r_id])
      next_num += 1


  def __savedata(self,r_id,uni_title,uni_text):


    diff_length = 10;
    store_title = escape(uni_title.encode("utf-8"))
    store_data  = escape(uni_text.encode("utf-8"))
    store_len   = len(uni_text)
    store_hash  = gethash(store_data)

    if self.db.exists_record(DATA,"r_id",r_id):
      # すでに r_id をキーとする レコードが存在するならば
      # hash をとっておくこのhashと比較して new flag を立てるか判断する材料にする
      oldhash = self.db.select("hash",DATA,"where r_id = %s" %r_id)[0][0]
      oldlen  = int(self.db.select("size",DATA,"where r_id = %s" %r_id)[0][0])
      # hash が違っていて ∧  diff_length 以上/以下 変化してたら newflag　をたてる
      flag = 1 if ((oldhash != store_hash) and (abs(oldlen - store_len) > diff_length)) else 0
      self.db.update(DATA,\
          [("title" , store_title),\
          ("data" , store_data),\
          ("size" , store_len),\
          ("hash",store_hash),\
          ("new",flag)],\
          "where r_id = %s" %r_id)
    else:
      self.db.insert(DATA,[r_id , store_title,store_data , store_len , store_hash , 1])


  def stamp(self,d_id,r_id):
    self.db.update(DMAPPER , [("vtime" , int(time.time()))] , "where d_id = %s" %d_id)
    self.db.execute("update rmapper set vtime = %s , counter = counter + 1 where r_id = %s" %(int(time.time()),r_id))

  def finish(self):
    self.db.close()



def crawl(f):
  c = Crawler(f)
  c.d_interval = 14
  c.c_interval = 5
  c.crawl_toplevel()


if __name__ == "__main__" :
  crawl(lambda x:x.endswith(".dendai.ac.jp"))
