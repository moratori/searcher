#!/usr/bin/env python
#coding:utf-8

import cgi
import cgitb
import os
import sys
import searcher.core.searcher.main as s;


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


def search(query):
  # query は unicode
  c = s.Searcher()
  res = c.search_and_toplevel(query,lambda q,d: (d if len(d) < 180 else d[0:179]))
  num = len(res)
  if num == 0:
    print "<br>「%s」を含む文書は見つかりませんでした。" %(query.encode("utf-8"))
  else:
    print "%s件のヒット<br>" %(num)
    for (url,title,data) in res:
      url = url.encode("utf-8")
      title = title.encode("utf-8")
      data = newline(data).encode("utf-8")
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

