#!/usr/bin/env python
#coding:utf-8

import CGIHTTPServer
import BaseHTTPServer

port = 43221
BaseHTTPServer.HTTPServer(("",port),CGIHTTPServer.CGIHTTPRequestHandler).serve_forever()
