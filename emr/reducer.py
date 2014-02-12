#!/usr/bin/env python

import sys

def main():
    a = []
    for line in sys.stdin:
        if line not in a:
            a.append(line)
            print line,
if __name__ == "__main__":
    main()