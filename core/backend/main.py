#!/usr/bin/env python
#coding:utf-8

import multiprocessing as m
import searcher.core.crawler.crawler as c;
import searcher.core.indexer.nounregister as i;
import sys


p1 = m.Process(target=c.crawl)
p2 = m.Process(target=i.indexing)

p1.start()
p2.start()

try:
  p1.join()
  p2.join()
except:
  p1.terminate()
  p2.terminate()
  sys.exit()

