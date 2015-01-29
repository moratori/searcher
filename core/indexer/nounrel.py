#!/usr/bin/env python
#coding:utf-8


import MeCab
import MySQLdb
import re


def cond(x):
  return (len(x) > 1) and (re.sub(re.compile("[!-/:-@[-`{-~]"),"",x) != "") and (not x.isdigit())

def getnoun(text , cond = lambda x: True):

  """
    condがTrueとなる名詞だけ取り出しUnicodeのリストを返す
  """

  tagger = MeCab.Tagger("-Ochasen")
  text = text.encode("utf-8")
  node   = tagger.parseToNode(text)
  result = []

  while node:
    if node.feature.split(",")[0] == "名詞":
      s = node.surface.strip()
      try:
        tmp = s.decode("utf-8")
        if ( s != "" )and cond(tmp): 
          result.append(tmp.lower())
      except:pass
    node = node.next
  return result


def makesemnet(nounlist , n):
  """
    ネットワークをつくる
  """
  result = {}
  tmp = []

  for (index , noun) in enumerate(nounlist):
    item = nounlist[index + 1 : index + n + 1]
    if item :
      for each in item:
        if each != noun:
          tmp.append((noun,each))

  while tmp:
    t = tmp.pop()
    cnt = tmp.count(t) + 1
    while t in tmp: tmp.remove(t)
    result[t] = cnt
  return result


def mergenet(net1,net2):
  """
    ２つのネットワークをマージした新しいネットワークを返す
  """
  new = {}
  for (k,v) in net1.items():
    if k in net2:
      new[k] = v + net2[k]
    else:
      new[k] = v
  for (k,v) in net2.items():
    if not k in new:
      new[k] = v
  return new


def main():
  """
    名詞の関連度テーブルにぶち込む
    これを実行する時は nounrelation テーブルは空の状態であることを意図している
  """

  (user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())

  c = MySQLdb.connect(host="localhost" , user=user,passwd=passwd ,db="searcher",charset="utf8")
  cur = c.cursor()

  cur.execute("drop table if exists nounrelation;")
  cur.execute("create table nounrelation(w1 varchar(8192) not null , w2 varchar(8192) not null , weight int default 0);")
  c.commit()

  cur.execute("select data from data");
  resultnet = {}

  for (data,) in cur.fetchall():
    resultnet = mergenet(makesemnet(getnoun(data,cond),1),resultnet)

  for (k,v) in resultnet.items():
    (a,b) = k
    cur.execute("insert into nounrelation(w1,w2,weight) values(\"%s\",\"%s\",%s)" %(a.encode("utf8"),b.encode("utf8"),v))

  c.commit()
  cur.close()
  c.close()
  return 


if __name__ == "__main__": main()




