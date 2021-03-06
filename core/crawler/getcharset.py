#!/usr/bin/env python
#coding:utf-8

from HTMLParser import HTMLParser 
import traceback


class ENDParse(Exception):

  def __init__(self,result):
    self.result = result

  def __str__(self):
    return repr(self.result)




class XMLGetCharset(HTMLParser):
  """
    start メソッドにxml文書をぶち込むとcharsetを返す
  """

  default = "utf-8"
  
  def __init__(self):
    HTMLParser.__init__(self)


  def handle_pi(self,data):
    t = filter(lambda x:x.startswith("encoding"),data.split(" "))
    if t:
      raise ENDParse(t[0].split("=")[1].strip("\"").lower())

  def start(self,target):
    try:
      self.feed(target)
    except ENDParse as e:
      return e.result
    except:pass
    return self.default
 


# head の meta タグ周辺にまでマルチバイト文字があったら
# どうなるかは保証しない
class HTMLGetCharset(HTMLParser):

  default_charset = "shift_jis"

  def __init__(self,raw):
    HTMLParser.__init__(self)
    self.raw = raw

  def handle_starttag(self,stag,attrs):
    if (stag == "meta"):
      tmp = dict([(each[0].lower() , each[1].lower()) for each in attrs]) 
      if ("http-equiv" in tmp) and (tmp["http-equiv"] == "content-type") and ("content" in tmp):
        result = HTMLGetCharset.getcharset(tmp["content"])
        if result: raise ENDParse(result)

  # http-header の mime 形式から charset を得る
  @staticmethod
  def getcharset(mime):
    value = map(lambda x:x.strip(),mime.split(";"))
    if len(value) > 1:
      charset = map(lambda x:x.strip(),value[1].split("="))
      if len(charset) > 1: 
        return charset[1]

  def start(self):
    try:
      self.feed(self.raw)
    except ENDParse as e:
      return e.result
    except:pass
    return self.default_charset

