#!/usr/bin/env python
#coding:utf-8

import MeCab
import logging
import math
from searcher.core.db.sqlutil import *



host = "localhost"
db = "searcher"

logging.basicConfig(filename="/home/moratori/Github/searcher/core/indexer/indexer.log")
(user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())


def getnoun(text):
  # 返すのはunicode型の名詞のリスト
  # と 重複を除去しなかった場合の名詞の数

  tagger = MeCab.Tagger("-Ochasen")
  text = text.encode("utf-8")
  node   = tagger.parseToNode(text)
  result = []
  unique = set()
  cnt = 0

  while node:
    if node.feature.split(",")[0] == "名詞":
      s = node.surface.strip()
      try:
        tmp = s.decode("utf-8")
        if (s != "") : cnt += 1
        if ( s != "" ) and (not tmp in unique): 
          unique.add(tmp)
          result.append(tmp)
      except:pass
    node = node.next
  return result , float(cnt)


def escape(text):
  # utf-8 な文字列しかできない
  return re.sub(re.compile("[!-/:-@[-`{-~]"),"",text)


class Indexer:

  def __init__(self):
    self.db = DB(host,user,passwd)
    self.db.open(db)

  # target となる新しいテキストをもってくる
  def gettarget(self):
    query = (["r_id","data"] , "data" , "where new = 1")
    target = apply(self.db.select , query)
    # DBにはutf-8 で入れてるんだけど帰ってくるときにはPython Unicodeになってる
    return target

  def registnoun(self,noun):
    # noun を登録して そのIDを返す
    noun = escape(noun)
    tmp = self.db.select("n_id" , "nmapper" , "where noun = \"%s\"" %(noun))
    if not tmp:
      tmp = self.db.select("max(n_id)" , "nmapper");
      ((val,),) = tmp
      num = val + 1 if val else 1
      self.db.insert("nmapper" , [num,noun])
      return num
    else:
      # すでに noun が存在する場合
      ((num,),) = tmp
      return num

  def registplace(self,n_id,r_id,indexes):
    self.db.delete("place" , "where (n_id = %s) and (r_id = %s)" %(n_id , r_id))
    for (num,index) in enumerate(indexes):
      self.db.insert("place" , [n_id,r_id,num+1,index])

  def registfreq(self,n_id,r_id,freq):
    if self.db.select("*" , "freq" , "where n_id = %s and r_id = %s" %(n_id , r_id)):
      self.db.update("freq" , [("freq",freq)] , "where (n_id = %s) and (r_id = %s)" %(n_id , r_id))
    else:
      self.db.execute("insert into freq(n_id,r_id,freq) values(%s,%s,%s)" %(n_id , r_id , freq))

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

  def finish(self):
    self.db.close()

  def indexing(self):
    # ここの data　は unicode
    for (r_id , data) in self.gettarget():
      # noun_list is unicode noun list! 
      (noun_list , cnt) = getnoun(data)

      # いままでに得られている 名詞のリストも考える
      for (n_id,noun) in self.db.select(["n_id","noun"] , "nmapper"):
        freq = (data.count(noun) / cnt)
        if freq == 0 : continue
        self.registfreq(n_id , r_id , freq)
        self.registplace(n_id , r_id , self.getoccurrence(noun,data))

      # data 自身が含んでいる名詞について考える
      for noun in noun_list:
        n_id = self.registnoun(noun.encode("utf-8"))
        freq = (data.count(noun) / cnt)
        if freq == 0 : continue
        self.registfreq(n_id , r_id , freq)
        self.registplace(n_id , r_id , self.getoccurrence(noun,data))

      self.indexed(r_id)
      self.db.commit()
    self.scoring()

  def scoring(self):
    ((tot_data_num,),)= self.db.select("count(*)" , "data")
    for (num , (n_id , r_id , tf)) in enumerate(self.db.select(["n_id","r_id","freq"] , "freq")):
      ((noun_included_num,),) = self.db.select("count(*)" , "freq" , "where n_id = %s" %n_id)
      idf = math.log(tot_data_num/float(noun_included_num),2)
      self.db.update("freq",[("tfidf" , tf * idf)] , "where (n_id = %s) and (r_id = %s)" %(n_id,r_id))
      # 1000 件毎に commit する. 毎回 commit してたら阿呆みたいに遅くなるので
      if ((num % 1000) == 0) : self.db.commit()
    self.db.commit()



def indexing():
  c = Indexer()
  try:
    c.indexing()
  except:pass
  finally:
    c.finish()


