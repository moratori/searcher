#!/usr/bin/env python
#coding:utf-8

import MeCab
import logging
import math
import traceback
import time
import datetime
from searcher.core.db.sqlutil import *



host = "localhost"
db = "searcher"

logging.basicConfig(filename="/home/moratori/Github/searcher/LOG/indexer.log")
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

  # scoring の処理は毎回行われるべきであるから 
  # この値は 0 とした
  scoring_interval = 0

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

      # sigma(s<-d ,  tf(s,d)) を求める。これが分母となる
      summing_tf = float(sum([data.count(noun) for noun in noun_list]))

      # cnt が 0 なのはおかしいのでそういうのは削除
      if cnt == 0:
        self.db.erase(r_id)
        continue

      # いままでに得られている 名詞のリストも考える
      # わざわざここで データベースから 名詞一覧を毎回もってこなくても
      # 追加分だけ Python側で 持っとけば必要ないな -> 何万もの名詞のリストをもっとくのは きつくないか?
      for (num , (n_id,noun)) in enumerate(self.db.select(["n_id","noun"] , "nmapper")):
        freq = (data.count(noun) / summing_tf)
        if freq == 0 : 
          continue
        else:
          self.registfreq(n_id , r_id , freq)
          self.registplace(n_id , r_id , self.getoccurrence(noun,data))
          if ((num % 300) == 0):self.db.commit()
        self.db.commit()

      # data 自身が含んでいる名詞について考える
      for noun in noun_list:
        n_id = self.registnoun(noun.encode("utf-8"))
        freq = (data.count(noun) / summing_tf)
        if freq == 0 : 
          continue
        else:
          self.registfreq(n_id , r_id , freq)
          self.registplace(n_id , r_id , self.getoccurrence(noun,data))

      self.indexed(r_id)
      self.db.commit()

  # socring は data や freq テーブルが変更されたなら、
  # 毎回行われるべきである
  def scoring(self):
    ((tot_data_num,),)= self.db.select("count(*)" , "data")
    now = int(time.time())
    for (num , (n_id , r_id , tf)) in enumerate(self.db.select(["n_id","r_id","freq"] , "freq" , "where (tfidf is null) or ((%s - tstamp) > %s)" %(now,self.scoring_interval))):
      ((noun_included_num,),) = self.db.select("count(*)" , "freq" , "where n_id = %s" %n_id)
      idf = math.log(tot_data_num/float(noun_included_num),2)
      now = int(time.time())
      self.db.update("freq",[("tfidf" , tf * idf),("tstamp",now)] , "where (n_id = %s) and (r_id = %s)" %(n_id,r_id))
      # 300 件毎に commit する. 毎回 commit してたら阿呆みたいに遅くなるので
      if ((num % 300) == 0) : self.db.commit()
    self.db.commit()



def indexing():
  c = Indexer()
  try:
    c.indexing()
    c.scoring()
  except:
    logging.error("\n" + str(datetime.datetime.today()) + "\n" + traceback.format_exc() + "\n")  
  finally:
    c.finish()
  return


if __name__ == "__main__" :
  indexing()
  
