#!/usr/bin/python3
from hash_functions import *
from net_functions import *
from  _thread import *
from socket import *
import threading
import hashlib
import random
import time
import sys
import os

"""
TODO List
add second successor and additonal protocol for that
test some more
"""

#figure out our own IP and Port
myIP = gethostbyname(gethostname())
myPort = random.randint(8000, 43000)
me = (myIP, myPort)
print('IP: {}\nPort {}'.format(myIP,myPort))

#basic things that we need &
#Things about the hash that we need to know/keep track of
running = True
myKeys = {} # contains {hash:string}
functionLock = threading.Lock() #lock up get, exists, insert, and remove
maxHash = (2**160)-1
fingers = [] #finger Table contains (ip,port)
successor = 0
successor2 = 0
myHash = getHashIndex(me)
myMaxHash = 0

def pulse(connInfo):
    try:
        conn = socket(AF_INET, SOCK_STREAM)
        conn.connect(connInfo)
        conn.sendall('PUL'.encode())
        response = recvAll(conn, 1).decode().lower()
        if response == 't':
            return True
        else:
            return False
    except Exception:
        return False

def updateFingerTable():
    global fingers
    global successor
    global successor2
    segment = maxHash/5
    if successor == me:
        #we're the only peer
        return
    if not pulse(successor):
        try:
            conn = socket(AF_INET, SOCK_STREAM)
            conn.connect(successor2)
            conn.sendall('ABD'.encode())
            successor = recvAddress(conn)
            conn.close()
        except Exception:
            #if we hit this exception that means that we don't know either successor
            #because they're both unresponsive in some way
            #which also means we're pretty much boned on this table
            #I have actually no idea how to properly handle this situation
            successor = me
    for i in range(0,5):
        if fingers[i]==me or not pulse(fingers[i]):
            hashToFind = random.randint(segment*i, segment*(i+1))
            if myHash > myMaxHash and (myHash<=hashToFind or myMaxHash>=hashToFind):
                fingers[i]=me
                continue
            elif myHash <= hashToFind <= myMaxHash:
                fingers[i]=me
                continue
            try:
                #the peer for the hash is not us
                currPeer = successor
                ownerFound = False
                while not ownerFound:
                    conn = socket(AF_INET, SOCK_STREAM)
                    conn.connect(currPeer)
                    conn.sendall('OWN'.encode())
                    sendKey(conn, hashToFind)
                    newPeer = recvAddress(conn)
                    conn.close()
                    if newPeer == currPeer:
                        ownerFound = True
                    else:
                        currPeer = newPeer
                fingers[i] = currPeer
            except Exception as e:
                print('Could not make change to finger table entry {}'.format(i))
                print(e)
                pass

def fingerTableHandler():
    while running:
        updateFingerTable()
        time.sleep(60)

def findLocalOwner(key):
    #check to make sure it isn't me
    if myHash > myMaxHash and (myHash<=key or key<=myMaxHash):
        #print('I own this data and I wrap around')
        #print('My hash:         {}'.format(myHash))
        #print('Data hash:       {}'.format(key))
        #print('Successor hash:  {}'.format(getHashIndex(successor)))
        return me
    elif myHash < myMaxHash and myHash <= key <= myMaxHash:
        #print('I own this data and I DO NOT wrap around')
        #print('My hash:         {}'.format(myHash))
        #print('Data hash:       {}'.format(key))
        #print('Successor hash:  {}'.format(getHashIndex(successor)))
        return me
    try:
        #print('Local peer with closest hash is not me')
        numAttempts = 0
        currPeer = successor
        currHash = getHashIndex(currPeer)
        while numAttempts < 4:
            currPeer = successor
            if myMaxHash < key:
                #find the closest peer if the hash if greater than our
                for i in range(0,5):
                    newHash = getHashIndex(fingers[i])
                    if newHash > currHash and newHash < key:
                        currHash = getHashIndex(fingers[i])
                        currPeer = fingers[i]
            else:
                #find the peer if its hash is less than ours
                for i in range(0,5):
                    newHash = getHashIndex(fingers[i])
                    if newHash<key and (currPeer==successor or newHash>currHash):
                        currHash = newHash
                        currPeer = fingers[i]
                if currPeer == successor:
                    for i in range(0, 5):
                        newHash = getHashIndex(fingers[i])
                        if newHash > currHash and fingers[i]!=me:
                            currHash = newHash
                            currPeer = fingers[i]
            if pulse(currPeer) and currPeer != me:
                #print('I do not own the data and have passed on the next peer')
                #print('My hash:                 {}'.format(myHash))
                #print('Successor hash:          {}'.format(getHashIndex(successor)))
                #print('Closest Local Peer Hash: {}'.format(getHashIndex(currPeer)))
                #print('Data hash:               {}'.format(key))
                return currPeer
            else:
                updateFingerTable()
                numAttempts+=1
        #print('Attempt limit exceeded. Assume Successor is closer than us.')
        return successor
    except Exception as e:
        print('Error in finding local owner.')
        print('Assume Successor is closer than us.')
        print(e)
        return successor

def findTrueOwner(key):
    ownerFound = False
    currOwner = findLocalOwner(key)
    if currOwner == me:
        #print('TRUE OWNER WAS ME')
        #print('My hash:         {}'.format(myHash))
        #print('Data hash:       {}'.format(key))
        #print('Successor hash:  {}'.format(getHashIndex(successor)))
        #print('------------------------------------')
        return me
    while not ownerFound:
        conn = socket(AF_INET, SOCK_STREAM)
        conn.connect(currOwner)
        conn.sendall('OWN'.encode())
        sendKey(conn, key)
        newPeer = recvAddress(conn)
        conn.close()
        if newPeer == currOwner:
            ownerFound = True
        else:
            currOwner = newPeer
    #print('TRUE OWNER')
    #print('My hash:         {}'.format(myHash))
    #print('Successor hash:  {}'.format(getHashIndex(successor)))
    #print('True Owner Hash: {}'.format(getHashIndex(currOwner)))
    #print('Data hash:       {}'.format(key))
    #print('------------------------------------')
    return currOwner

def buildInfo():
    #send finger table info as [string size][string]
    string = 'Me:        {} {}\nSuccessor: {} {}\n'.format(me, myHash, successor, getHashIndex(successor))
    string +='Successor2:{} {}\n'.format(successor2, getHashIndex(successor2))
    string += '*Note* Successor 2 has a very high chance of being inaccurate due to protocol implementation.\n'
    string += '\nFingerTable:\n'
    for i in range(0,5):
        string += '[{}]:       {} {}\n'.format(i, fingers[i], getHashIndex(fingers[i]))
    string += '\nKey-Value:\n'
    for key in myKeys:
        string += '{}:{}\n'.format(key, myKeys[key])
    return string

def keyExists(key):
    if key in myKeys.keys():
        return True
    else:
        return False

def disconnect():
    try:
        functionLock.acquire()
        #find peer at (myHash-1)
        #connect and send [dis][me]
        owner = findTrueOwner(myHash-1)
        conn = socket(AF_INET, SOCK_STREAM)
        conn.connect(owner)
        conn.sendall('DIS'.encode())
        sendAddress(conn, me)
        #recv a [T/F]
        response = recvAll(conn, 1).decode().lower()
        if response != 't':
            conn.close()
            updateFingerTable()
            functionLock.release()
            disconnect()
            return
        #send [successor]
        sendAddress(conn, successor)
        sendAddress(conn, successor2)
        #send numItems and each item
        sendInt(conn, len(myKeys))
        for key in myKeys:
            sendKey(conn, key)
            sendVal(conn, myKeys[key].encode())
        #receive [T]
        response = recvAll(conn, 1).decode().lower()
        if response != 't':
            print('something went very wrong while disconnecting')
        functionLock.release()
    except Exception as e:
        functionLock.release()
        print('An error occured while disconnecting')
        print(e)

def handleLocalClient():
    global running
    while running:
        try:
            cmd = input('Enter a command: ').strip().lower()
            if cmd == 'insert':
                #find out which key
                key = input('Name the data you would like to insert: ')
                key=int.from_bytes(hashlib.sha1(key.encode()).digest(), byteorder="big")
                #find out data
                data = input('What would you like to insert?: ')
                owner = findTrueOwner(key)
                print('{}:{}\nto be inserted to {}'.format(key,data,owner))
                #send key
                conn = socket(AF_INET, SOCK_STREAM)
                conn.connect(owner)
                conn.sendall('ins'.encode())
                sendKey(conn, key)
                #wait for T/F/N
                #if response == T: send data
                response = recvAll(conn, 1).decode().lower()
                if response == 't':
                    sendVal(conn, data.encode())
                    response = recvAll(conn, 1).decode().lower()
                    if response == 't':
                        print('data inserted to table')
                    elif response == 'f':
                        print('Data could not be inserted into table by peer')
                    else:
                        print('Error: inknown message received')
                elif response == 'n':
                    print('Error: peer does not own this space. Cannot insert.')
                else:
                    print('Error: unknown message received.')
                conn.close()
            elif cmd == 'own':
                key = input('Owns what?: ')
                key=int.from_bytes(hashlib.sha1(key.encode()).digest(), byteorder="big")
                print('{}'.format(findTrueOwner(key)))
            elif cmd == 'remove':
                #ask for key to remove
                key = input('What key to remove?: ')
                key=int.from_bytes(hashlib.sha1(key.encode()).digest(), byteorder="big")
                owner = findTrueOwner(key)
                #send key
                conn = socket(AF_INET, SOCK_STREAM)
                conn.connect(owner)
                conn.sendall('rem'.encode())
                sendKey(conn, key)
                #wait for T/F/N
                response = recvAll(conn, 1).decode().lower()
                if response == 't':
                    print('key removed')
                elif response == 'f':
                    print('key does not exist')
                elif response == 'n':
                    print('Error: Key not owned by expected owner')
                else:
                    print('Error: unknown message received.')
                conn.close()
            elif cmd == 'get':
                #ask for key to get
                key = input('What key to get?: ')
                key=int.from_bytes(hashlib.sha1(key.encode()).digest(), byteorder="big")
                owner = findTrueOwner(key)
                #send key
                conn = socket(AF_INET, SOCK_STREAM)
                conn.connect(owner)
                conn.sendall('get'.encode())
                sendKey(conn, key)
                #recv F/N and drop OR
                #   recv [T][valsize][val]
                response = recvAll(conn, 1).decode().lower()
                if response == 't':
                    print(recvVal(conn).decode())
                elif response == 'f':
                    print('key does not exist')
                elif response == 'n':
                    print('Error: Key not owned by expected owner')
                else:
                    print('Error: unknown message received.')
                conn.close()
            elif cmd == 'exists':
                #ask for key to get
                key = input('What key to check?: ')
                key=int.from_bytes(hashlib.sha1(key.encode()).digest(), byteorder="big")
                owner = findTrueOwner(key)
                #send key
                conn = socket(AF_INET, SOCK_STREAM)
                conn.connect(owner)
                conn.sendall('exi'.encode())
                sendKey(conn, key)
                #wait for T/F/N
                response = recvAll(conn, 1).decode().lower()
                if response == 't':
                    print('key exists')
                elif response == 'f':
                    print('key does not exist')
                elif response == 'n':
                    print('Error: Key not owned by expected owner')
                else:
                    print('Error: unknown message received.')
                conn.close()
            elif cmd == 'info':
                try:
                    #ask which peer in table
                    string = '[0] Me\n[1] Successor\n'
                    for i in range(0,5):
                        if fingers[i] != me:
                            string += '[{}] {}\n'.format(i+2, fingers[i])
                        else:
                            string += '[{}] Me\n'.format(i+2)
                    print(string)
                    peer = int(input('Which peer?: '))
                    #connect to that peer and call [inf]
                    #recv[stringSize][string]
                    #print the info
                    if peer < 0 or peer > 7:
                        print('Invalid answer')
                        continue
                    elif peer == 0:
                        print(buildInfo())
                    elif peer == 1:
                        conn = socket(AF_INET, SOCK_STREAM)
                        conn.connect(successor)
                        conn.sendall('INF'.encode())
                        print(recvVal(conn).decode())
                    elif fingers[peer-2] == me:
                        print(buildInfo())
                    else:
                        conn = socket(AF_INET, SOCK_STREAM)
                        conn.connect(fingers[peer-2])
                        conn.sendall('INF'.encode())
                        print(recvVal(conn).decode())
                except Exception:
                    print('Something went wrong. Please try again later.')
            elif cmd == 'exit':
                if successor == me:
                    print('I am the only peer in the table. I am disconnecting.')
                    print('Please enter (Ctrl + c) to finish disconnecting')
                    running = False
                else:
                    disconnect()
                    running = False
                    print('Disconnected from DHT. If program does not close, it is safe to (Ctrl+c)')
            else:
                print('Invalid command. Please try one of the following:')
                print('[insert, remove, get, own, exists, info, exit]')
        except Exception as e:
            print(e)
            print('An error has occured with the local peer')

def handleClient(connInfo):
    global successor
    global successor2
    global myMaxHash
    global myKeys
    conn, connAddress = connInfo
    try:
        cmd = recvAll(conn, 3).decode().lower()
        #print('Received {} from {}'.format(cmd, connAddress))
        if cmd == 'ins':
            functionLock.acquire()
            #recv key
            key = recvKey(conn)
            #sent T/N depending on if we own the space
            owner = findLocalOwner(key)
            if owner == me and findTrueOwner(key)==me:
                conn.sendall('T'.encode())
                #recv value and insert into dict
                #send T/F
                try:
                    myKeys[key] = recvVal(conn).decode()
                    conn.sendall('T'.encode())
                except Exception:
                    conn.sendall('F'.encode())
            else:
                conn.sendall('N'.encode())
            #recv value and insert into dict
            #send T/F
            functionLock.release()
        elif cmd == 'abd':
            sendAddress(successor)
        elif cmd == 'rem':
            functionLock.acquire()
            #recv key
            key = recvKey(conn)
            #respond T/F/N
            owner = findLocalOwner(key)
            if owner == me and keyExists(key):
                conn.sendall('T'.encode())
                del myKeys[key]
            elif owner == me:
                conn.sendall('F'.encode())
            else:
                conn.sendall('N'.encode())
            functionLock.release()
        elif cmd == 'get':
            functionLock.acquire()
            #recv key
            key = recvKey(conn)
            #respond F/N and close conn OR
            #   respond [T][valsize][val]
            owner = findLocalOwner(key)
            #print('Requested data owner: {}'.format(owner))
            if owner == me and keyExists(key):
                conn.sendall('T'.encode())
                sendVal(conn, myKeys[key].encode())
            elif owner == me:
                conn.sendall('F'.encode())
            else:
                conn.sendall('N'.encode())
            functionLock.release()
        elif cmd == 'exi':
            functionLock.acquire()
            #recv key
            key = recvKey(conn)
            # respond T/F/N
            owner = findLocalOwner(key)
            if owner == me and keyExists(key):
                conn.sendall('T'.encode())
            elif owner == me:
                conn.sendall('F'.encode())
            else:
                conn.sendall('N'.encode())
            functionLock.release()
        elif cmd == 'own':
            #find closest peer address
            #send address
            key = recvKey(conn)
            keyOwner = findLocalOwner(key)
            sendAddress(conn, keyOwner)
        elif cmd == 'con':
            functionLock.acquire()
            try:
                #recv address
                #send T/N depending on if we own that space
                client = recvAddress(conn)
                if findLocalOwner(getHashIndex(client)) != me:
                    conn.sendall('N'.encode())
                conn.sendall('T'.encode())
                #send successor
                if successor == me and successor2 == me:
                    #we do this if we are the only client in the table
                    successor2 = client
                sendAddress(conn, successor)
                sendAddress(conn, successor2)
                #send numItems followed by items
                successorHash = getHashIndex(successor)
                clientHash = getHashIndex(client)
                keysToSend = {}
                for key in myKeys:
                    if clientHash <= key < successorHash:
                        keysToSend[key] = myKeys[key]
                sendInt(conn, len(keysToSend))
                for key in keysToSend:
                    sendVal(conn, keysToSend[key].encode())
                #recv T from client
                response = recvAll(conn, 1).decode().lower()
                conn.close()
                if response == 't':
                    #print('My Old Successor: {}'.format(successor))
                    successor2 = successor
                    successor = client
                    for i in range(0, 5):
                        if fingers[i]==me:
                            fingers[i]=successor
                    #print('My New Successor: {}'.format(successor))
                    myMaxHash = clientHash-1
                    for key in keysToSend:
                        del myKeys[key]
                    functionLock.release()
                    print('Client {} has joined the table'.format(successor))
                    print('My Hash:        {}'.format(myHash))
                    print('Their Hash:     {}'.format(clientHash))
                    print('Successor Hash: {}'.format(successorHash))
                else:
                    print('Client failed to properly connect.')
                    print('Resuming normal function.')
                    functionLock.release()
            except Exception as e:
                print('A client tried to connect and failed')
                print(e)
                functionLock.release()
                conn.close()
                return
        elif cmd == 'dis':
            try:
                functionLock.acquire()
                #recv [clientAddr]
                peer = recvAddress(conn)
                #respond[T/N] based on if they are our successor
                if peer != successor:
                    conn.sendall('N'.encode())
                conn.sendall('T'.encode())
                #recv new successor address
                newSuccessor = recvAddress(conn)
                newSuccessor2 = recvAddress(conn)
                #recv numItems and each item
                numItems = recvInt(conn)
                for i in range(0, numItems):
                    key = recvKey(conn)
                    item = recvVal(conn).decode()
                    myKeys[key] = item
                #send 'T'
                conn.sendall('T'.encode())
                successor = newSuccessor
                successor2 = newSuccessor2
                if successor2 == peer or not pulse(successor2):
                    #this should ensure that nothing strange happens when
                    #only 2 peers exist and one of them disconnects
                    successor2 = me
                myMaxHash = getHashIndex(successor)-1
                functionLock.release()
                updateFingerTable()
            except Exception as e:
                functionLock.release()
                print('Error with peer trying to diconnect')
                print(e)
        elif cmd == 'pul':
            #send back confirmation that we are not dead
            conn.sendall('T'.encode())
        elif cmd == 'inf':
            #send finger table info as [string size][string]
            sendVal(conn, buildInfo().encode())
        conn.close()
        return
    except Exception as e:
        print('An error with the following connection: {}'.format(connAddress))
        print(e)
        conn.close()
        return


if len(sys.argv) == 1:
    print('Initializing DHT')
    myMaxHash = myHash-1
    successor = me
    successor2 = me
    for i in range(0,5):
        fingers.append(me)

elif len(sys.argv) == 3:
    print('Attempting to join DHT')
    try:
        #find owner
        #send [con][my address]
        ownerFound = False
        owner = ( (sys.argv[1], int(sys.argv[2])) )
        while not ownerFound:
            conn = socket(AF_INET, SOCK_STREAM)
            conn.connect(owner)
            conn.sendall('OWN'.encode())
            sendKey(conn, myHash)
            newOwner = recvAddress(conn)
            if newOwner == owner:
                ownerFound = True
            else:
                owner = newOwner
        conn = socket(AF_INET, SOCK_STREAM)
        conn.connect(owner)
        conn.sendall('CON'.encode())
        sendAddress(conn, me)
        response = recvAll(conn, 1).decode().lower()
        if response != 't':
            print('Something went wrong. Please try again')
            conn.close()
            exit()
        #recv successor's address
        successor = recvAddress(conn)
        successor2 = recvAddress(conn)
        #recv numItems followed by each item
        numItems = recvInt(conn)
        for i in range(0, numItems):
            key = recvKey(conn)
            myKeys[key] = recvVal(conn).decode()
        conn.sendall('T'.encode())
        conn.close()
        #update relevant information
        myMaxHash = getHashIndex(successor)-1
        for i in range(0,5):
            fingers.append(me)
        updateFingerTable()
        print('Successfully joined DHT')
    except Exception as e:
        print('Could not connect to DHT')
        print(e)
        print('Shutting Down')
        exit()
else:
    print('Invalid Command Line Arguments')
    print('Shutting Down')
    exit()

#Make a new thread that will handle the local client
start_new_thread(handleLocalClient, ())
start_new_thread(fingerTableHandler, ())

# Set up listening socket
listener = socket(AF_INET, SOCK_STREAM)
listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
listener.bind(('', myPort))
listener.listen(32) # Support up to 32 simultaneous connections

while running:
    try:
        #accept connections from other clients
        threading.Thread(target=handleClient, args=(listener.accept(),), daemon=True).start()
    except Exception as e:
        print(e)
        print('Error occurred on listener')
    except KeyboardInterrupt:
        print('Shutting down listener')
        running = False
