#!/usr/bin/env python
#coding:utf-8

import cgi
import urllib


our = "http://localhost/"

form = cgi.FieldStorage()
url  = form.getvalue("url",our)


print "Status: 302 Found"
print "Content-Type: text/html"
print "Location: %s" %urllib.unquote_plus(url)
print ""
