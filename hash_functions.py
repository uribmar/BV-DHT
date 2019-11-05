#!/usr/bin/python3

import hashlib

#########################
#########################
##                     ##
##  Hash-Related Code  ##
##                     ##
#########################
#########################


# Returns an integer index into the hash-space for a node Address
#  - addr is of the form ("ipAddress or hostname", portNumber)
#    where the first item is a string and the second is an integer
def getHashIndex(addr):
    b_addrStr = ("%s:%d" % addr).encode()
    return int.from_bytes(hashlib.sha1(b_addrStr).digest(), byteorder="big")




