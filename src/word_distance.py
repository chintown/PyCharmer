#!/usr/bin/python

# from itertools import imap
# import operator
import difflib


def cliff(str1, str2):
    # remove all the ' ' and ',' in both string
    str1, str2 = map(lambda s: s.lower().replace(' ', '').replace(',', ''), [str1, str2])
    s = difflib.SequenceMatcher(None, str1, str2)
    return 1.0 - s.ratio()


def printMatrix(m):
    '''
    display method for levenshtein
    '''
    print ''
    for line in m:
        spTupel = ()
        breite = len(line)
        for column in line:
            spTupel = spTupel + (column, )
        print "%3i" * breite % spTupel


def levenshtein(s1, s2):
    l1 = len(s1)
    l2 = len(s2)

    matrix = [range(l1 + 1)] * (l2 + 1)
    for zz in range(l2 + 1):
        matrix[zz] = range(zz, zz + l1 + 1)
    for zz in range(0, l2):
        for sz in range(0, l1):
            if s1[sz] == s2[zz]:
                matrix[zz + 1][sz + 1] = min(matrix[zz + 1][sz] + 1, matrix[zz][sz + 1] + 1, matrix[zz][sz])
            else:
                matrix[zz + 1][sz + 1] = min(matrix[zz + 1][sz] + 1, matrix[zz][sz + 1] + 1, matrix[zz][sz] + 1)
    ##printMatrix(matrix)
    return matrix[l2][l1]

if __name__ == "__main__":
    s1 = 'abcdistribut'
    s2 = 'abcdistributin'
    s3 = 'dog'
    s4 = 'god'
    print cliff(s1, s2)
    print cliff(s3, s4)
    #print lev(s1,s2)

    print levenshtein(s1, s2)
    print levenshtein(s3, s4)
