#!/usr/bin/python
#coding:utf-8


print "Content-Type: text/html\n"

import cgi
import cgitb

cgitb.enable()

import os
import sys
import searcher.core.searcher.main as s;
import time
import urllib
import urlparse
import xml.sax.saxutils





INVALID_METHOD = "<html>invalid method</html>"
NO_KEYWORD     = "<html>no keyword</html>"
METHOD = "GET"
FOOTER = "</body></html>"

FORM = """
  <div id = "sform">
      <form style = "display: inline;" name = "main" action="/cgi-bin/search.py" onSubmit="return check()">
        <input type="radio" name = "domain" value = "0" %s>全て
        <input type="radio" name = "domain" value = "1" %s>電大トップ
        <input type="radio" name = "domain" value = "2" %s>SIE
        <input type="radio" name = "domain" value = "3" %s>CSE
        <br><br>
        <input type="text" name="keyword" class="word" value = "%s">
        <input type="submit" value="検索" class="button">
      </form>
      <br>
      <form style = "display: inline;" name = "suggest">
        <input name = "relnoun" id = "suggest" type="text" disabled>
      </form>

      <script>changed();</script>
  </div>
"""

HEADER = '''<html>
  <head>
    <meta http-equiv="Conten-Type" content="text/html; charset=UTF-8">
    <script type = "text/javascript" src = "/js/suggest.js"></script>
    <link rel="stylesheet" href="/style/sresult.css" type="text/css">
    <title>大学内検索エンジン</title>
  </head>
  <body>
    <div id = "header"><a href="http://web.dendai.ac.jp/" target="_blank"><img src="/img/logo.gif"></a></div>
'''


def_domain = {0: "" , 1: "web.dendai.ac.jp" , 2: "sie.dendai.ac.jp" , 3: "cse.dendai.ac.jp"}



def newline(data):
  s = u""
  for (num , c) in enumerate(data):
    if num % 47 == 0 and num != 0:
      s += "<br>" + c
    else:
      s += c
  return s

def fixlen(text , length , dot = False):
  flag = False
  if len(text) > length:
    flag = True
    tmp = text[0:length]
  else:
    tmp = text
  return tmp + "....." if dot and flag else tmp

# text に含まれるキーワドをふと文字にし
# 長さを 先頭180文字くらいにする
def keyword_bold(queries,text,l = 160):
  # テキストは unicode
  tmp = newline(fixlen(text , l))
  return (reduce(lambda r,x:r.replace(x,u"<B>" + x + u"</B>") ,queries , tmp))


def output_page(query , offset , now , domain):
  print ("""
  <a href = "/cgi-bin/search.py?keyword=%s&page=%s&domain=%s" style ="text-decoration: none;"><button type = "submit" style = "width: 32;height: 32;">%s</button></a>&nbsp;""" %(query.encode("utf-8") , offset , domain ,(("<b><font color = \"maroon\">" + str(offset) + "</font></b>") if (offset == now) else offset)))


def output_buttons(enumnum,pageoff,hits,query,domain):
  # 1 2 3 4 5 ... 30
  # 2 3 4 5 6 7 ... 30
  # 各ページへのリンク
  pnum= hits / enumnum
  rest = hits % enumnum
  pnum = (pnum + 1) if rest != 0 else pnum 
  # 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30
  # 分割表示数にする場合の数 sp
  sp  = 15
  # 前後の表示する数
  bf  = 7
  if (pnum > sp):
    start = (pageoff - bf) if pageoff > bf else 1
    end   = sp if (start == 1) else ((pageoff + bf) if pageoff < pnum - bf else pnum)
    for n in range(start-1 , end):
      output_page(query , n + 1 , pageoff , domain)
  else:
    for n in range(pnum): 
      output_page(query , n + 1 , pageoff , domain)


def search(query,pageoff,domain):
  # query は unicode
  enumnum = 9
  c = s.Searcher()
  domstr = def_domain[domain] if domain in def_domain else def_domain[0]
  start = time.time()
  
  # other_words は別の検索語を表すユニコード文字列のリスト
  # qwords_list はqueryを空白で区切ったうにコード文字列のリスト
  (res , hits , other_words,qwords_list) = c.search_and_toplevel(query , domstr , digestmaker = keyword_bold , pageoff=pageoff , default = enumnum)

  interval = time.time() - start

  if other_words:
    print "<div id = \"other\">"
    print "他のキーワード: "
    for each in other_words:
      (a,b) = (each[0].encode("utf8") , each[1].encode("utf8"))
      print "<a href =\"/cgi-bin/search.py?keyword=%s %s\">%s %s</a>" %(a , b,a,b)
    print "</div>"

  if hits == 0:
    print "<div id = \"info\">「<b>%s</b>」を含むウェブページは見つかりませんでした</div>" %(query.encode("utf-8"))
  else:
    print "<div id = \"info\">「<b>%s</b>」の検索結果 <b>%s</b> ページ目 <b>%s</b>件のヒット( <b>%.4f</b> 秒 )</div>" %(query.encode("utf-8") , pageoff ,hits , interval)

    print "<div id = \"main\">"
    for (number , (url,title,data,md_id)) in enumerate(res):
      url = url.encode("utf-8")
      title = fixlen(xml.sax.saxutils.escape(title), 50 , dot = True).encode("utf-8")
      data = data.encode("utf-8")
      uobj = urlparse.urlparse(url)
      print "<div class=\"each\">"

      print ("<div class=\"title\"><a href = \"/cgi-bin/redirect.py?url=%s&rank=%s&keyword=%s\" title=\"%s\" target = \"_blank\">%s</a>&nbsp;&nbsp;%s</div>" 
                %(urllib.quote_plus(url) , 
                  number+1,
                  urllib.quote_plus(u" ".join(qwords_list).encode("utf-8")),
                  xml.sax.saxutils.escape(url) ,
                  "&lt;UNTITLED WEB PAGE&gt;" if (title == "") else title,
                  ("" if not md_id else 
                  "<img src=\"/cgi-bin/favicon.py?d_id=%s\" width=\"16\" height=\"16\">" %md_id)))

      print "<div class=\"url\">%s</div>" %(xml.sax.saxutils.escape(fixlen(url , 60 , dot = True)) )
      print "<div class=\"abst\">%s</div>" %(data)
      print "</div>"
    print "</div>"

    print "<div id = \"pages\">"
    output_buttons(enumnum , pageoff,hits,query,domain)
    print "</div>"

  c.finish()


def fillform(form , domain , query):
  """
    前回のドメイン検索の値と検索ワードでフォームを埋める
  """
  tmp = ["" if each != domain else "checked" for each in range(len(def_domain))]
  tmp.append(xml.sax.saxutils.escape(query.encode("utf-8")))
  return form %tuple(tmp)

def main(environ):
  if (not "REQUEST_METHOD" in environ) or (environ["REQUEST_METHOD"] != METHOD):
    print INVALID_METHOD
    return
  try:
    field = cgi.FieldStorage()
    query = field.getvalue("keyword","").decode("utf-8")
    page  = field.getvalue("page" , "").decode("utf-8")
    domain = field.getvalue("domain" , "0").decode("utf-8")
    page   = (int(page) if (int(page) > 0) else 1) if page.isdigit() else 1
    domain = int(domain) if domain.isdigit() else 0 
    if query == "" : 
      print NO_KEYWORD
      return
  except:
    print NO_KEYWORD
    return
  print HEADER + fillform(FORM , domain , query)
  search(query,page,domain)
  print FOOTER


main(os.environ)
sys.exit()

