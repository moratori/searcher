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
import socket
import pickle
import os
import base64
import zipfile

from HTMLParser import HTMLParser
from searcher.core.db.sqlutil import *



random.seed()


DMAPPER = "dmapper"
RMAPPER = "rmapper"
DATA    = "data"
LINKR   = "linkr"
BLACK   = "black"
WHITE   = "white"


logging.basicConfig(filename="/home/moratori/Github/searcher/LOG/crawler.log")



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
  return (link.scheme + "://" + link.netloc).encode("utf-8",errors="ignore")

def getpath(link):
  path = link.path
  query = link.query
  result = ((path if query == "" else path + "?" + query).encode("utf-8",errors="ignore"))
  return "/" if result == "" else result

def qescape(text):
  return re.sub(re.compile("[\"\'\\\]"),"",text)

def escape(text):
  # utf-8 な文字列しかできない
  return re.sub(re.compile("[!-/:-@[-`{-~]"),"",text)


def decode_to_unicode(text,est):
  default = [est,"shift_jis","utf-8"]
  for codec in default:
    try:
      tmp = text.decode(codec)
      return tmp
    except:pass
  return text.decode(est,errors="ignore")


 


def analizepptx(path,proc):
  """
    path(pptxファイル)の一枚ずつの slide[0-9].xml の文字列
    をprocに与えて呼び出す

    procは自分で xmlファイルのエンコーディングを調べて適切に処理する1引数関数
  """
  result = u""
  with zipfile.ZipFile(path,"r") as zf:
    target = zf.namelist()
    for each in target:
      if re.match("ppt/slides/slide([0-9])+\.xml",each):
        with zf.open(each,"r") as xmlfile:
          result += proc(xmlfile.read()) + u" "
  return result



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
       "wmv","wma","wma","wav","mp4","mp3","mid","midi","mov","mpg","mpeg","avi","swf" ,\
       "xls","xlsx","doc","docx","ppt",\
       "zip","rar","lzh","gz","z","cab",\
       "css","js","xml","txt","exe","csv"]) 

  exclude_extension = exclude_extension.union(set([each.upper() for each in exclude_extension]))


  def __init__(self,url,mtype,rawhtmldata):
    HTMLParser.__init__(self)

    self.charset       = detect.HTMLGetCharset.getcharset(mtype)
    # HTMLGetCharsetクラスが頑張ってmetaタグの中みて charset を得るけど、絶対合ってるとはいえないけど
    # その場合はどうしようか
    self.charset       = detect.HTMLGetCharset(rawhtmldata).start() if not self.charset else self.charset
    self.rawhtml       = decode_to_unicode(rawhtmldata,self.charset) 
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




class XMLAnalizer(HTMLParser):

  """
    handle_data に与えられたデータをくっつけて返す
    但しゴミはのぞく 半角記号,半角数字,1文字のなにかがゴミ
  """

  def __init__(self,raw):
    HTMLParser.__init__(self)
    self.raw    = decode_to_unicode(raw,detect.XMLGetCharset().start(raw))
    self.result = u""

  def handle_data(self,data):
    tmp = re.sub(re.compile("[!-@[-`{-~]"),"",data.strip())
    if tmp != u"" and len(tmp) != 1:
      self.result += tmp + u" "

  def start(self):
    self.feed(self.raw)
    return self.result





class Crawler:
    # コンテンツにアクセスするときのwait
  c_interval = 4

  # 接続要求出して待つ時間
  timeout = 15
  
  # User-Agent は IE9
  useragent = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"

  mime_type = ("text/html","application/pdf","application/vnd.openxmlformats-officedocument.presentationml.presentation")

  def __init__(self,host,port,user,passwd,db):
    self.host = host
    self.port = port

    self.db = DB(host,user,passwd)
    self.db.open(db)



  def __nexttarget(self):
    s = socket.socket(socket.AF_INET , socket.SOCK_STREAM , socket.IPPROTO_TCP)
    s.connect((self.host , self.port))

    sfile = s.makefile()

    (_,size) = sfile.readline().split(":")

    result = pickle.loads(sfile.read(int(size)))

    sfile.close()
    s.close()

    return result



  def crawl_toplevel(self):
    try:
      roots = self.__nexttarget()
    except:
      logging.warning("\nconnect failed: " + str(datetime.datetime.today()) + "\nto controller, retry later ...\n")
      return

    while roots:
      node = roots.pop(0)
      self.crawl_favicon(node)
      self.crawl(node)

    self.finish()
    return 


  ###
  ### favicon のクロールで primary key がダブルことがおこるっぽい
  def crawl_favicon(self,node):
    """
      node は全て同じドメインのurlなのでここで
      そのファビコンを得る。
      pathの違いでドメインのファビコンが様々変わることは無いことを意図している
    """
    if node.isempty():return
    target = node.container[0]
    d_id   = target.d_id
    urlobj = urlparse.urlparse(target.url)
    favicon_url = urlparse.urlunparse((urlobj.scheme,urlobj.netloc,"favicon.ico","","",""))
    try:
      data = urllib2.urlopen(favicon_url,timeout=1).read()
    except:return
    self.db.execute("insert into favicon values(%s,'%s')" %(d_id,base64.b64encode(data)))
    self.db.commit()
    return

 

  def crawl(self,node):
    assert isinstance(node,Node)
    # node は全て同じ ドメインだったはずなので適当にwaitをかける
    for t in node.container:
      try:
        self.analyze(t.r_id,t.d_id,t.url)
        # 1URLアクセスするごとに commit することにした
        self.db.commit()
      except:
        logging.error("\nunexpected error: " + str(datetime.datetime.today()) + "\n" + traceback.format_exc() + "\n")
      time.sleep(random.randint(1,self.c_interval))


  def getcontent(self,url,connection,mtype):
    data = connection.read()
    if   mtype.startswith("text/html"):
      return HTMLAnalizer(url,mtype,data).start()
    elif mtype.startswith("application/pdf"):
      result = u""
      tmpfile = str(datetime.datetime.today()).replace(" ","-").replace(":","-").replace(".","-")
      resfile = tmpfile + "result"
      with open(tmpfile , "w") as f:
        f.write(data)
      os.system("pdf2txt.py -c utf8 -o %s %s" %(resfile , tmpfile))
      with open(resfile , "r") as f:
        for line in f.read().decode("utf-8").split("\n"):
          tmp = line.strip()
          if tmp : result += tmp + u" "
      os.remove(tmpfile)
      os.remove(resfile)
      return (u"",result,[])
    elif mtype.startswith("application/vnd.openxmlformats-officedocument.presentationml.presentation"):
      tmpfile = str(datetime.datetime.today()).replace(" ","-").replace(":","-").replace(".","-")
      with open(tmpfile , "w") as f:
        f.write(data)
      result = analizepptx(tmpfile , lambda raw: XMLAnalizer(raw).start())
      os.remove(tmpfile)
      return (u"",result,[])
    else:
      # getcontentメソッドが呼ばれる前に mtype が self.mime_typeで始まる事を意図しているので
      # のでここにはこない
      assert False



  # 実際に url にアクセスしてDBに保存したりする処理のコントローラ
  def analyze(self,r_id,d_id,url):
    try:
      print "Accessing: " , url
      req = urllib2.Request(url,headers={"User-Agent": self.useragent})
      connection = urllib2.urlopen(req,timeout=self.timeout)
      mtype = connection.info().getheader("Content-Type")
      # content-type が明示されていない若しくは
      # text/html でないなら DBから抹消
      if (not mtype) or (not mtype.startswith(self.mime_type)):
        self.db.erase(r_id)
        self.db.commit()
        return
    except:
      logging.warning(("can't connect url: %s" %url) + "\n" +  str(traceback.format_exc()))
      return
    try:
      (uni_title,uni_text,links) = self.getcontent(url,connection,mtype)
    except:
      self.db.erase(r_id)
      self.db.commit()
      logging.warning(("unparsible contents. erase r_id = %s from DB" %r_id) + "\n" + traceback.format_exc() + "\n\n")
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
        self.db.insert(DMAPPER , [dname , 0] , "(name,vtime)")

    # +++++++ rmapper に登録する ++++++ #
    child_r_ids = []
    for link in links: 
      d_id  = self.db.revlookup_dname(getdname(link))
      assert (d_id)
      path = qescape(getpath(link))
      cand = get_indexcand(path)
      already = self.db.exists_rmapper(d_id,cand)
      if not already:
        # ここで 新しいキーをもらえるはずなのに
        # duplicateでprimary key の制約に違反しておちてるケースが
        # ログにあった
        self.db.insert(RMAPPER , [d_id , path , 0 , 0] , "(d_id,path,vtime,counter)")
        ((new_r_id,),) = self.db.select("r_id" , RMAPPER , "where (d_id = %s) and (path = '%s')" %(d_id,path))
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
    store_title = escape(uni_title.encode("utf-8",errors="ignore"))
    store_data  = escape(uni_text.encode("utf-8",errors="ignore"))
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



def crawl():
  db         = "searcher"
  controller = "localhost"
  port       = 12345 
  (user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())

  while True:
    Crawler(controller , port , user , passwd , db).crawl_toplevel()
    time.sleep(random.randint(1,12))


if __name__ == "__main__" : crawl()
