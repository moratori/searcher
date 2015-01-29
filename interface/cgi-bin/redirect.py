#!/usr/bin/env python
#coding:utf-8

import cgi
import cgitb
import urllib

cgitb.enable()

our = "http://localhost/"

form = cgi.FieldStorage()
url  = form.getvalue("url",our)
rank = form.getvalue("rank","0")
keyword = form.getvalue("keyword","")


print "Status: 302 Found"
print "Content-Type: text/html"
print "Location: %s" %urllib.unquote_plus(url)
print ""
