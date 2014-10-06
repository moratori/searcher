#!/usr/bin/env python
#coding:utf-8

import cgi
import cgitb
import os
import sys
import searcher.core.searcher.main as s;
import time


cgitb.enable()


INVALID_METHOD = "<html>invalid method</html>"
NO_KEYWORD     = "<html>no keyword</html>"
METHOD = "GET"
FOOTER = "</body></html>"

FORM = """
  <form action="/cgi-bin/search.py" method = "GET">
    <input type = "radio" name = "domain" value = "0" %s>全て
    <input type = "radio" name = "domain" value = "1" %s>電大トップ
    <input type = "radio" name = "domain" value = "2" %s>SIE
    <input type = "radio" name = "domain" value = "3" %s>CSE
    <br><br>
    <input size = "55" type = "text" name = "keyword">
    <input type = "submit" value = "Search">
  </form>
"""

HEADER = '''
<html>
<head>
  <meta http-equiv = "Content-Type" content = "text/html; charset=utf8">
  <title>results for %s</title>
  <style type = "text/css">
    <!--
      #header {
        text-align: center;  
      }
      #main{
        text-align: center;  
      }
    -->
  </style>
</head>
<body>
  <div id = "header">
  <h2>学内検索エンジン</h2>
'''


def_domain = {0: "" , 1: "web.dendai.ac.jp" , 2: "sie.dendai.ac.jp" , 3: "cse.dendai.ac.jp"}


print "Content-Type: text/html\n"



def newline(data):
  s = u""
  for (num , c) in enumerate(data):
    if num % 47 == 0 :
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
  <a href = "/cgi-bin/search.py?keyword=%s&page=%s&domain=%s" style ="text-decoration: none;"><button type = "submit" style = "width: 32;height: 32;">%s</button>&nbsp;&nbsp;""" %(query.encode("utf-8") , offset , domain ,(("<b><font color = \"maroon\">" + str(offset) + "</font></b>") if (offset == now) else offset)))


def search(query,pageoff,domain):
  # query は unicode
  enumnum = 9
  c = s.Searcher()
  domstr = def_domain[domain] if domain in def_domain else def_domain[0]
  start = time.time()
  (res , hits) = c.search_and_toplevel(query , domstr , digestmaker = keyword_bold , pageoff=pageoff , default = enumnum)
  interval = time.time() - start
  if hits == 0:
    print "<br>「<b>%s</b>」を含むウェブページは見つかりませんでした。</div>" %(query.encode("utf-8"))
  else:
    print "「<b>%s</b>」の検索結果 %s ページ目 %s件のヒット( %.4f 秒 ) <br><br></div><div id = \"main\">" %(query.encode("utf-8") , pageoff ,hits , interval)
    for (number , (url,title,data)) in enumerate(res):
      url = url.encode("utf-8")
      title = fixlen(title, 50 , dot = True).encode("utf-8")
      data = data.encode("utf-8")
      print "<br>"
      print "<a href = \"%s\" target = \"_blank\">%s</a><br>" %(url , "UNTITLED" if (title == "") else title)
      print "<font size = \"2\" color = \"green\">%s</font>" %(fixlen(url , 60 , dot = True))
      print "<font size = \"2\">%s</font>" %(data)
      print "<br><br>"
    print "<br>"
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

    print "</div><br>"
  c.finish()


def checked_radio(form , domain):
  return form %tuple(["" if each != domain else "checked" for each in range(len(def_domain))])

def main(environ):
  if (not "REQUEST_METHOD" in environ) or (environ["REQUEST_METHOD"] != METHOD):
    print INVALID_METHOD
    return
  try:
    field = cgi.FieldStorage()
    query = field.getvalue("keyword","").decode("utf-8")
    page  = field.getvalue("page" , "").decode("utf-8")
    domain = field.getvalue("domain" , "1").decode("utf-8")
    page   = (int(page) if (int(page) > 0) else 1) if page.isdigit() else 1
    domain = int(domain) if domain.isdigit() else 0 
    if query == "" : 
      print NO_KEYWORD
      return
  except:
    print NO_KEYWORD
    return
  print (HEADER % query.encode("utf-8")) + checked_radio(FORM , domain)  + "<hr>"
  search(query,page,domain)
  print FOOTER


if __name__ == "__main__":
  main(os.environ)
  sys.exit()

