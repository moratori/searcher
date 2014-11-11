#!/usr/bin/env python
#coding:utf-8

import cgi
import MySQLdb



(user,passwd) = map(lambda x:x.strip(),open("/home/moratori/Github/searcher/.pwd").readlines())


con = MySQLdb.connect(host="localhost",passwd=passwd,user=user,db="searcher",charset="utf8")
cur = con.cursor()

print "Content-Type: text/plain\n"

field = cgi.FieldStorage()
query = field.getvalue("p","")

cur.execute("select w2 from nounrelation where w1 = '%s' order by weight desc limit 5" %(query))

res = "   ".join([w2 for (w2,) in cur.fetchall()])

print res.encode("utf8")

cur.close()
con.close()



