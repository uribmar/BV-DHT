# BV-DHT
A Dynamic Hash Table written in Python. 

## Initializing the Application
1. Make the program an executable using the following:
```bash
chmod +x bvDHT.py
```
2. Initialize one instance of the application
On startup, the application will display the IP address and port number thast the application is running on.
```bash
./bvDHT.py
```
3. Connect additional clients
```bash
./bvDHT {IP Address} {Port Number}
```

## Commands
### help
Displays the possible commands listed below.
### insert
Asks for a key and its value. That key and value are then iinserted into the table. 
If they key already existed, the data at that key is overwritten by the new data.
### remove
Asks for a key then removes that key from the table.
### get
Asks for a key then prints the data that exists at that key.
### own
Asks for a key then tells the user the IP address and port number of the client that holds that key.
### exists
Asks for a key then tells the user whether or not the key is in the table.
### info
Displays the finger table for a given peer selected from a list.
### exit
Closes the connection that the client holds to the dynamic hash table and transfers data over to a new owner.
