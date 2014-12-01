#!/usr/bin/env python
#coding:utf-8

import MySQLdb
import base64
import cgi
import cgitb


(user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())
form = cgi.FieldStorage()
d_id = form.getvalue("d_id","")


if d_id:
  con = MySQLdb.connect(host="localhost",db="searcher",user=user,passwd=passwd)
  cur = con.cursor()
  cur.execute("select data from favicon where d_id = %s" %d_id)
  # d_id が本当に存在するか
  encoded = cur.fetchall()[0][0]
  raw = base64.b64decode(encoded)
  print "Content-Type: image/x-icon"
  print "Content-Length: %s" %len(raw)
  print ""
  print raw
else:
  print "Content-Type: text/plain"
  print ""
  print "BadAccess"
