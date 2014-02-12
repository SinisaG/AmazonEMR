#!/usr/bin/env python

import sys

def main(argv):
    for line in sys.stdin:
        print '%d' % int(line)

if __name__ == "__main__":
    main(sys.argv)