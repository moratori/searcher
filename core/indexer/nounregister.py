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


# 本文にたくさん出現する noun を log で抑えるんじゃなくて
# 本文の長さに応じてピークが違う関数を作るべきではない?
# -> 多く出現することで抑えこむのは IDF の仕事では
def count_noun(noun , title , data , param = 1.25):
  # title も考慮して出現頻度を数える
  tmp = data.count(noun)
  return ((len(noun) * param) if (title.count(noun) > 0) else 0) + (0 if tmp == 0 else math.log(tmp+1,4.5))


class Indexer:

  # scoring の処理は毎回行われるべきであるから 
  # この値は 0 とした
  scoring_interval = 0


  english_stop = set([u"of",u"for",u"is",u"am",u"are",u"the",u"an",u"a",u"as",u"in",u"by",u"on",u"to",u"at",u"it",u"its",u"up"])

  def __init__(self):
    self.db = DB(host,user,passwd)
    self.db.open(db)

  # target となる新しいテキストをもってくる
  def gettarget(self):
    query = (["r_id","title","data"] , "data" , "where new = 1")
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
      return num , True
    else:
      # すでに noun が存在する場合
      ((num,),) = tmp
      return num , False

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

  def estimate_title(self,r_id,noun_list):
    """
      タイトルの無いコンテンツのために、本文からタイトルを推定する
    """
    return u""

    counter = {}
    for each in noun_list:
      if each in counter:
        counter[each] += 1
      else:
        counter[each] = 1
    k = counter.keys()
    cand = sorted(k,key=lambda x:-counter[x])
    title = u""
    for each in cand:
      if not self.stop_word(each):
        title = each
        break
    title = cand[0] if not title else title
    self.db.execute("update data set title = \"%s\" where r_id = %s" %(title,r_id))
    self.db.commit()

    return title

  def indexing(self):
    
    # これ 空リストで初期化してたけど
    # 毎回index作成処理を初めからやることを仮定してしまってるので
    # ここで初めに nmapper の名詞をとっとこう
    all_noun_list = list(self.db.select(["n_id","noun"] , "nmapper")) # []

    # ここの data　は unicode
    for (r_id , title , data) in self.gettarget():
      # noun_list is unicode noun list! 
      (noun_list , cnt) = getnoun(data)

      len_nlist = len(noun_list)

      # 名詞がないリソースはいらない
      if (len_nlist == 0) or (cnt == 0):
        self.db.erase(r_id)
        self.db.commit()
        continue
      
      
      # r_id がタイトルｎ無いコンテンツだった場合のために
      # 推定を行なう
      if not title: 
        title = self.estimate_title(r_id,noun_list)


      avg_tf = 0
      cnt    = 0
      for (_ , noun) in all_noun_list:
        tmp = count_noun(noun , title , data)
        if tmp > 0:
          cnt += 1
          avg_tf += tmp
      avg_tf /= (1 if cnt == 0 else cnt)

      # いままでに得られている 名詞のリストも考える
      # avg_tf が0 って事は既存の名詞を含まない様な文書ということなので
      if avg_tf != 0:
        for (num , (n_id,noun)) in enumerate(all_noun_list): #enumerate(self.db.select(["n_id","noun"] , "nmapper")):
          freq  = (count_noun(noun , title , data)) / avg_tf 
          if freq == 0 : 
            continue
          else:
            self.registfreq(n_id , r_id , freq)
            self.registplace(n_id , r_id , self.getoccurrence(noun,data))
            if ((num % 400) == 0):self.db.commit()
        self.db.commit()

      # avg(s<-d ,  tf(s,d)) を求める。これが分母となる
      avg_tf = reduce(lambda r,x: r + count_noun(x,title,data) , noun_list , 0)/float(len_nlist)

      # data 自身が含んでいる名詞について考える
      for noun in noun_list:
        # ストップワードやゴミについては索引を作らない
        if self.stop_word(noun): continue
        (n_id , new) = self.registnoun(noun.encode("utf-8"))
        if new : all_noun_list.append((n_id,noun))
        freq  = (count_noun(noun , title , data)) / avg_tf 
        if freq == 0 : 
          continue
        else:
          self.registfreq(n_id , r_id , freq)
          self.registplace(n_id , r_id , self.getoccurrence(noun,data))
      self.indexed(r_id)
      self.db.commit()

  # ストップワードであるかをチェックする
  # 日本語の文章をmecabにかけている文にはいいんだけど
  # 英語の文章だと困る
  def stop_word(self,noun):
    return (noun in self.english_stop) or ((len(noun) == 1) and (not (re.match(u"[a-zA-Z0-9]",noun) is None)))

  # socring は data や freq テーブルが変更されたなら、
  # 毎回行われるべきである
  # tfidf 値を求めて、freqテーブルのtfidfカラムにぶち込む
  def scoring(self):
    ((tot_data_num,),)= self.db.select("count(*)" , "data")
    now = int(time.time())
    for (num , (n_id , r_id , tf)) in enumerate(self.db.select(["n_id","r_id","freq"] , "freq" , "where (tfidf is null) or ((%s - tstamp) > %s)" %(now,self.scoring_interval))):
      ((noun_included_num,),) = self.db.select("count(*)" , "freq" , "where n_id = %s" %n_id)
      idf = math.log(tot_data_num/float(noun_included_num),10)
      now = int(time.time())
      self.db.update("freq",[("tfidf" , tf * idf),("tstamp",now)] , "where (n_id = %s) and (r_id = %s)" %(n_id,r_id))
      # 400 件毎に commit する. 毎回 commit してたら阿呆みたいに遅くなるので
      if ((num % 400) == 0) : self.db.commit()
    self.db.commit()

  # PageRankで ランク付けを行う
  def __pageranking(self,network,initscore = 100.0,limit=7):
    nodes = network.keys()
    result = {node: initscore for node in nodes}
    for d in range(limit):
      tmp = {node:0 for node in nodes}
      for node in nodes:
        length = len(network[node])
        # もし出ていく方向のリンクが0なら
        if length == 0 : continue
        out = result[node] / length 
        for c in network[node]:
          # C は必ずしもtmpにない可能性がある?
          if c in tmp:tmp[c] += out
        result[node] = 0
      result = tmp
    return result

  # page rank によるウェブページのランク付けを行う
  def pagerank_toplevel(self):
    network = {}
    init = None
    for (r_id,num,child) in self.db.select("*","linkr"):
      if (init is None) or (r_id != init):
        if not (init is None):
          network[init] = tmp
        tmp = []
        unique = set()
      init = r_id
      if not child in unique:
        tmp.append(child)
        unique.add(child) 
    ranking = self.__pageranking(network)
    # 後は このranking をテーブルにぶち込めばいい
    for (num, node)in enumerate(ranking.keys()):
      self.db.insert("pagerank" , [node,float("%.5f" %ranking[node])])
      if (num % 400) == 0 : self.db.commit()
    self.db.commit()


def indexing(clear=True):
  u"""
  
    crawlして古いdataが更新された場合は テーブルを初期化してからindexを作ら無ければいけない

    逆にまだ古くなっていない場合は差分だけ(新しくnewフラグがたったものだけ)indexingしてやればいいんだけど
    現状の実装だと毎回インデックスを全て作成しなおしている.(全データをnewにしている)

  """
  c = Indexer()

  if clear:
    c.db.execute("drop table if exists freq;")
    c.db.execute("drop table if exists place;")
    c.db.execute("drop table if exists pagerank;")
    
    c.db.execute("update data set new = 1;")
    
    c.db.execute("create table freq(n_id int , r_id int , freq float ,tfidf float, tstamp int default 0 ,primary key(n_id , r_id));")
    c.db.execute("create table place (n_id int , r_id int , num int , place int , primary key(n_id , r_id , num));")
    c.db.execute("create table pagerank(r_id int not null primary key , rank float default 0);")
    
    c.db.commit()

  try:
    # 名詞あつめて nmapper に登録
    # place テーブルにも出現位置とかいれる
    c.indexing()
    # 各名詞とウェブページに対するTFIDFを計算する
    c.scoring()
    # PageRankを計算する 
    c.pagerank_toplevel()
  except:
    logging.error("\n" + str(datetime.datetime.today()) + "\n" + traceback.format_exc() + "\n")  
  finally:
    c.finish()
  return


if __name__ == "__main__" : indexing(clear=False)

