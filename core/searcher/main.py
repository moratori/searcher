#!/usr/bin/env python
#coding:utf-8

import MeCab
import logging
import math
import re
import urlparse
from searcher.core.db.sqlutil import *



host = "localhost"
db = "searcher"

logging.basicConfig(filename="/home/moratori/Github/searcher/LOG/searcher.log")
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

def sorted_in(elm,lst):
  if not lst:return False
  for each in lst:
    if each > elm : return False
    if each == elm : return True
  return False



def avg_mergesort(a,b,siglevel = 0.6):
  length = float(len(a))
  dic ={}
  for (index , elm) in enumerate(a):
    pair_index = b.index(elm)
    diff = abs(pair_index - index)/length
    if diff > siglevel:
      dic[elm] = min(index,pair_index)
    else:
      dic[elm] = (index + pair_index)/2.0
  return sorted(a,key = lambda x: dic[x])



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

  def search_word(self , word, domstr):
    # word は unicode を意図している
    # 登録されていだろう名詞wordについて検索し、
    # その 名詞ID と、 名詞を含むリソースを表すr_idとtfidf値の組み (r_id,tfidf) のリストを返す
    word = escape(word.encode("utf-8"))
    tmp = self.db.select("n_id","nmapper","where noun = \"%s\"" %(word))
    if not tmp : return None , []
    ((n_id,),) = tmp
    self.db.execute("select r_id,tfidf from freq as tmp1 where (n_id = %s) and (exists (select r_id,d_id from rmapper as tmp2 where (tmp2.r_id = tmp1.r_id) and (exists (select * from dmapper where (tmp2.d_id = d_id) and (name like \"%%%s\")))))" %(n_id,domstr))
    return n_id , self.db.cursor.fetchall() #self.db.select(["r_id" , "tfidf"] , "freq" , "where n_id = %s" %n_id)


  def __count_phrase(self,r_id,word,splited):
    # r_id で表される文書に phrase が何回出現するかを返す
    # ("情報環境学部" , ["情報" , "環境" , "学部"])
    n_ids = []
    phrase = []
    for noun in splited:
      # ここで noun はかならず nmapper に存在する
      # 存在しないやつは search_andメソッドのreturn で帰ってるから
      n_id = self.db.select("n_id" , "nmapper" , "where noun = \"%s\"" %noun)[0][0]
      tmp = self.db.select("place" , "place", "where (n_id = %s) and (r_id = %s) order by place asc" %(int(n_id),int(r_id)))
      phrase.append((len(noun) , [place for (place,) in tmp]))
      # phrase = [(2,[1,2,3,4,6,3,2,1]) , (2,[4,3,2,5,1,5,3]) ...]
      # ここの phrase 変数は フレーズを１つ表している "情報環境学部" とか
    def countphrase_main(lis):
      if not lis or len(lis) == 1: return 0
      def path(index , l):
        if not l: return True
        (length , c) = l[0]
        return path(length+index,l[1:]) if sorted_in(index , c) else False
      ((length , root) , res) = (lis[0] , 0)
      for start in root:
        if path(start+length,lis[1:]):res += 1
      return res
    return countphrase_main(phrase)

  def __phrase_sort(self,r_id_list,phrase_dic):
    # ここのr_id_listはtfidf値を元にソートされたr_idのリスト
    # 各 r_id は phrase_dic から生成される noun のフラットリストの名詞を全て含んでいる
    # これらはソートはされているけど、形態素の出現頻度を元に評価されただけだから
    # フレーズでもソートしてやる -> どうやって?
    
    # pjrase_dic とかいって {"就職": ["就職"] , "情報": ["情報"]}
    # みたいな辞書だったら フレーズソートするいみないよね　
    if every(lambda k: len(phrase_dic[k]) == 1, phrase_dic.keys()) : 
      return r_id_list
    else:
      res = {}
      for r_id in r_id_list:
        for (phrase , splited) in phrase_dic.items():
          # phrase の長さと 出現回数の積を得点にしよう
          # やっぱり純粋なカウントだと(要するにフレーズ版のTF)よろしくない
          # スクールバス 情報環境学部 で検索して 情報環境学部だけ多く含む文書がきてしまう ...
          # やっぱり カウントはあんま考慮しないように log かけてしまおう?
          # 本当ならフレーズ版の TFIDF　を作りたいけどそれかなり大変だなー
          # データベースの定義からおおきくいろいろ変えないといけなくなりそう ... 
          # 長い語であっても特定的でないヤツはある? -> 情報環境学部とか
          value = len(phrase) * self.__count_phrase(r_id , phrase , splited)
          if r_id in res:
            res[r_id] += value
          else:
            res[r_id]  = value
      result = sorted(r_id_list , key = lambda x: res[x])
      result.reverse()
      return result




  def split(self,query):
    # query は Python unicode
    # query = "情報環境学部 情報通信サービス研究室 研究内容"みたいなの
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
    return res_dic , res_unique , words


  def __tfidf_sort(self,pages):
    # pages = [((r_id , tfidf), ... ) , ...]
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


  def sort_toplevel(self , pages , phrase_dic):
    # pages = [((r_id1 , tfidf) , (r_id2 , tfidf), ...) , ... , ]
    # 各要素は pages[i] はキーワード K_i で検索した時にそれを含むページとそれのtfidf値のリスト
    # 効率化のために __tfidf_sort 部分でtfidfで重みを付けるのと加えて and 処理までしてしまっている　
    # なので 後に続く ソート処理は tfidf_sort が返した結果に大してしかできないね...

    tfidf_sorted          = self.__tfidf_sort(pages)
    #phrase_sorted        = self.__phrase_sort(tfidf_sorted , phrase_dic) 
    #pagerank_full_sorted = self.__pagerank_sort(tfidf_sorted , n = len(tfidf_sorted))
    return tfidf_sorted

  def getrank(self,r_id):
    # r_id の PageRankを得る
    # PageRank テーブルに存在しない場合は 0 をかえせばいい
    res = self.db.select("rank" , "pagerank" , "where r_id = %s" %r_id)
    if not res: return 0
    return res[0][0]

  # ページランク表より r_id のリストをソートする
  # 但しこのソートはかなり特殊で n で限られたブロックの中しかソートしない
  def __pagerank_sort(self,r_id_list,n=3):
    # ここの n のあたいでtfidfでソートされてるリストをブロックにわけて
    # そのなかでpagerankによりソートする
    l = len(r_id_list)
    target = zip(*[iter(r_id_list)] * n)
    if (l % n != 0):
      target.append(r_id_list[-(l%n):])
    target = map(list,target)
    result = []
    for container in target:
      # container を ソートしてフラットにしたものを result に追加してく
      for r_id in reversed(sorted(container , key=lambda r_id: self.getrank(r_id))):
        result.append(r_id)
    return result


  def regist_query(self,l):
    if len(l) < 2 : return

    for w1 in l:
      for w2 in l:
        if w1 != w2:
          (a,b) = (w1.encode("utf8") , w2.encode("utf8"))
          if self.db.select("*" , "sword" , "where w1 = '%s' and w2 = '%s'" %(a,b)):
            self.db.execute("update sword set counter = counter + 1 where w1 = '%s' and w2 = '%s'" %(a,b))
          else:
            self.db.execute("insert into sword(w1,w2) values('%s','%s')" %(a,b))
        else:pass
    self.db.commit()

  # 併せてよく検索されるワードを探す
  def other_words(self,word):
    return self.db.select(["w1","w2"] , "sword" , "where w1 = '%s' order by counter desc limit 3" %word.encode("utf8"))


  def search_and(self , query , domstr):
    # query は Python unicode で 検索窓に入力された、半角|全角スペースで区切られた文字列
    (phrase_dic , flat_nlist , words) = self.split(query)
    # とりあえず、 flat_nlist だけみて 検索することにする. phrase検索はその後にする
    # 各単語∈ flat_nlist で検索した結果
    
    # words は ["情報環境学部" , "スクールバス" , "時間"]
    # みたいなユニコードのリスト

    qwords_list = sorted(words)
    self.regist_query(qwords_list[0:3])
    

    tmp = []
    for noun in flat_nlist:
      (n_id , rlist) = self.search_word(noun , domstr)
      # nmapper に存在しないような名詞だったら n_id はNone に成るわけだけど
      # そうなったら 検索結果は存在しない and 検索だから
      if (not n_id) or (not rlist): return ([],[],[],[])
      # ここで n_id をappend してないので n_id は落ちてしまう
      # つまり なにで検索されたのかわからなくなる
      tmp.append(rlist)
    return (self.sort_toplevel(tmp,phrase_dic) , flat_nlist , self.other_words(words[0]),qwords_list)

  def exists_favicon(self,d_id):
    """
      d_idのためのファビコンが存在するか否か判定
    """

    return d_id if self.db.select("d_id","favicon","where d_id = %s" %d_id) else False

  # digestmaker は unicode の本文文字列,unicodeのqueryをうけとって
  # 本文の要約をつくる関数
  def search_and_toplevel(self,query,domstr,digestmaker = (lambda a,b,c=160: b),pageoff=1,default=12):
    def part(r_id_list):
      tmp = pageoff * default
      # tmp-default の数は user がおかしな番号いれてこなければ
      # 大丈夫だけどそのへん考慮してないくそ
      (start , end) = (tmp - default , tmp)
      start = start if (start > 0) and (len(r_id_list) > start ) else 0
      return r_id_list[start:end] if (len(r_id_list) >= end) else r_id_list[start:]
    result = []
    # unicode もじの url,title,data を返す
    (r_id_list , flat_nlist , other_words,qwords_list) = self.search_and(query,domstr)
    for r_id in part(r_id_list):
      tmp = self.db.select(["title","data"] , "data" , "where r_id = %s" %r_id)
      (title,data) = tmp[0]
      d_id = self.db.lookup_d_id(r_id)
      (domain,path) = self.db.lookup_url(r_id)
      url = urlparse.urljoin(domain,path)
      result.append((url,title,digestmaker(flat_nlist,data),self.exists_favicon(d_id)))
    return result , len(r_id_list) , other_words , qwords_list




