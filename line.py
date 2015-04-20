#!/usr/bin/python

import os.path
import glob

src_path = [ "src", "test" ]

def linecnt(path, detail):
	children = glob.glob(path + "/*")
	cnt = 0
	for p in children:
		if os.path.isfile(p):
			f = open(p)
			lc_cnt = 0
			for line in f:
				cnt += 1
				lc_cnt += 1
			f.close
			if detail: print p, lc_cnt, "lines"
			
		elif os.path.isdir(p):
			if detail: print p
			cnt = cnt + linecnt(p, detail)
	return cnt

if __name__ == '__main__':
	print "* monodb code lines"
	print "---------------------------------------"
	total = 0
	for s in src_path:
		cnt = linecnt("./" + s, False)
		print "* line in", s, ":", cnt
		total = total + cnt
		
	print "---------------------------------------"
	print "* Total line number is", total
