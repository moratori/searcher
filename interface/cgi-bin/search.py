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
    <input size = "55" type = "text" name = "keyword">
    <input type = "submit" value = "Search">
  </form>
"""
HEADER = '''
<html>
<head>
  <meta http-equiv = "Content-Type" content = "text/html; charset=utf8">
  <title>result for %s</title>
</head>
<body>
  <br>
''' + FORM + "<hr>"

print "Content-Type: text/html\n"



def newline(data):
  s = u""
  for (num , c) in enumerate(data):
    if num % 70 == 0 :
      s += "<br>" + c
    else:
      s += c
  return s

# text に含まれるキーワドをふと文字にし
# 長さを 先頭180文字くらいにする
def keyword_bold(queries,text,l = 200):
  # テキストは unicode
  tmp = newline(text[0:l] if len(text) > l else text)
  return (reduce(lambda r,x:r.replace(x,u"<B>" + x + u"</B>") ,queries , tmp))

def search(query):
  # query は unicode
  c = s.Searcher()
  start = time.time()
  res = c.search_and_toplevel(query,keyword_bold)
  interval = time.time() - start
  num = len(res)
  if num == 0:
    print "<br>「%s」を含むウェブページは見つかりませんでした。" %(query.encode("utf-8"))
  else:
    print "「%s」の検索結果 %s件のヒット( %.4f 秒 )<br>" %(query.encode("utf-8") , num , interval)
    for (url,title,data) in res:
      url = url.encode("utf-8")
      title = title.encode("utf-8")
      data = data.encode("utf-8")
      print "<br>"
      print "<a href = \"%s\" target = \"_blank\">%s</a><br>" %(url , title)
      print "<font size = \"2\" color = \"green\">%s</font>" %(url)
      print "<font size = \"2\">%s</font>" %(data)
      print "<br>"
    print "<br><hr>" + FORM
  c.finish()


def main(environ):
  if (not "REQUEST_METHOD" in environ) or (environ["REQUEST_METHOD"] != METHOD):
    print INVALID_METHOD
    return
  try:
    query = cgi.FieldStorage().getvalue("keyword","").decode("utf-8")
    if query == "" : 
      print NO_KEYWORD
      return
  except:
    print NO_KEYWORD
    return
  print HEADER % query.encode("utf-8")
  search(query)
  print FOOTER


if __name__ == "__main__":
  main(os.environ)
  sys.exit()

