#!/usr/bin/env python3.4

import sys

def generateWords(line, delims):
    startidx = 0
    curidx = 0
    while curidx < len(line):
        if line[curidx] in delims:
            yield line[startidx:curidx]
            while curidx < len(line) and line[curidx] in delims:
                curidx += 1
            startidx = curidx
        curidx += 1
    yield line[startidx:]

while True:
    line = sys.stdin.readline().lower()
    # Break on EOF.
    if line == "":
        break
    for word in generateWords(line, delims= ' \n\t'):
        if len(word) > 0:
            print('%s\t%s' % (word, word))

            
sys.stdout.flush()