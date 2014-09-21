#!/usr/bin/env python
#coding:utf-8

import MeCab
import logging
import math
import re
from searcher.core.db.sqlutil import *



host = "localhost"
db = "searcher"

logging.basicConfig(filename="/home/moratori/Github/searcher/core/indexer/indexer.log")
(user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())



def split(s):
  # s は unicode
  return re.split(u"[ 　]" , s)


def escape(text):
  # utf-8 な文字列しかできない
  return re.sub(re.compile("[!-/:-@[-`{-~]"),"",text)

def every(f,s):
  for each in s:
    if not f(each):return False
  return True


def getnoun(text):
  # 返すのはunicode型の名詞のリスト
  # と 重複を除去しなかった場合の名詞の数
  tagger = MeCab.Tagger("-Ochasen")
  text = text.encode("utf-8")
  node   = tagger.parseToNode(text)
  result = []

  while node:
    if node.feature.split(",")[0] == "名詞":
      s = node.surface.strip()
      try:
        tmp = s.decode("utf-8")
        if ( s != "" ): result.append(tmp)
      except:pass
    node = node.next
  return result


class Searcher(DB):

  def __init__(self):
    self.db = DB(host,user,passwd)
    self.db.open(db)


  def finish(self):
    self.db.close()

  def search_word(self , word):
    # word は unicode を意図している
    # 登録されていだろう名詞wordについて検索し、
    # その 名詞ID と、 名詞を含むリソースを表すr_idとtfidf値の組み (r_id,tfidf) のリストを返す
    word = escape(word.encode("utf-8"))
    tmp = self.db.select("n_id","nmapper","where noun = \"%s\"" %(word))
    if not tmp : return None , []
    ((n_id,),) = tmp
    return n_id , self.db.select(["r_id" , "tfidf"] , "freq" , "where n_id = %s" %n_id)

  def split(self,query):
    # query は Python unicode
    # query = "情報環境学部 情報通信サービス研究室 研究内容"
    # -> {"情報環境学部": ["情報","環境","学部"] , "情報通信サービス研究室": ["情報","通信","サービス","研究","室"] , "研究内容": ["研究","内容"]}
    #    ["情報","環境","学部","通信","サービス","研究","室"]
    # フレーズ検索を加味したいのならば初めの返却値の辞書をかんがえてやれば良いと思うけども
    # 当然助詞は抜けてるので完全なフレーズ検索はできないかな
    words = split(query)
    res_dic = {}
    res_unique = []
    tmp_unique = set()
    # ここのwords は対した大きさにならない
    for each in words:
      noun_list = getnoun(each)
      res_dic[each] = noun_list
      for noun in noun_list:
        if not noun in tmp_unique:
          tmp_unique.add(noun)
          res_unique.append(noun)
    return res_dic , res_unique

  def scoring_and(self,pages):
    # pages = [((r_id , tfidf),...) , ...]
    # r_id のリストを返す(もちろんtfidf値でソートされた順)
    data = map(dict,pages)
    result_dic = {}
    if len(data) < 1:return []
    target_dic = data[0]
    # r_id は全ての辞書のkeyとなっていなければならない
    # そしてそのような r_id の値を足しこむ
    for r_id in target_dic.keys():
      total_score = 0
      for dic in data: 
        val = dic.get(r_id,None)
        if val:
          total_score += val
        else:break
      if val:result_dic[r_id] = total_score
    # スコアを元にr_id をソートする
    result = sorted(result_dic.keys(),key=lambda x: result_dic[x])
    result.reverse()
    return result

    
  def search_and(self , query):
    # query は Python unicode で 検索窓に入力された、半角|全角スペースで区切られた文字列
    (phrase_dic , flat_nlist) = self.split(query)
    # とりあえず、 flat_nlist だけみて 検索することにする. phrase検索はその後にする
    # 各単語∈ flat_nlist で検索した結果
    tmp = []
    for noun in flat_nlist:
      (n_id , rlist) = self.search_word(noun)
      if (not n_id) or (not rlist):continue
      # ここで n_id をappend してないので n_id は落ちてしまう
      # つまり なにで検索されたのかわからなくなる
      tmp.append(rlist)
    return self.scoring_and(tmp)


