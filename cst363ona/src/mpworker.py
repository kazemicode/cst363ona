import socket, threading, sys, traceback
import mysql.connector
#
#  to start a worker program, use the command
#      python mpworker.py 8000
#  where 8000 is the port number. 
#  use a different port number for each worker.
#  the port number must be one of the ports in the config file.


# worker configuration
# USERID, PASSWORD, hosts, ports and DEBUG are
# read from config file.  So don't edit these values here, 
# edit the config file.
USERID=''
PASSWORD=''
hosts=[]
ports=[]
DEBUG = 1  # DEBUG=0 none,  DEBUG=1 summary,  DEBUG=2 details

hostname = '127.0.0.1'
port = int(sys.argv[1])  # read port number from command line


class Worker(threading.Thread):
    def __init__(self, clientsocket):
        super().__init__()
        self.clientsocket = clientsocket
        self.cnx = mysql.connector.connect(user=USERID, password=PASSWORD,
                                database=('cst363'+str(port)), host='127.0.0.1')
        self.mapresult = None      # used to hold result set from a map operation

    def run(self):
        try:
            while True:
                in_msg = self.recv(self.clientsocket)
                out_msg = 'NOT OK'
                if in_msg == -1:
                    return
                    
                # in_msg is one of the following
                # map <sql select>
                # shuffle <insert statement>
                # reduce <sql select>
                # select ...
                # insert, create, drop or some other sql statement
                
                # get the first token of message
                ind = in_msg.index(' ')
                cmd = in_msg[0 : ind]
                
                if cmd == 'map':
                    # execute the sql select, hold the result set for now
                    sql = in_msg[ind:].strip() 
                    if DEBUG >= 2: 
                         print(sql)
                    cursor = self.cnx.cursor()
                    cursor.execute(sql)
                    self.mapresult=cursor.fetchall()
                    out_msg="OK"
                    
                elif cmd == "shuffle":
                    # using result set from prior map
                    # hash each row of result set and send it to worker task
                    # to insert into table on that worker
                    
                    in_msg = in_msg[ind:].strip()
                    wsockets = [None]*len(hosts)
                    try:
                        for row in self.mapresult:
                            # determine which worker to send the data to
                            worker_index = hash(row[0])%len(ports)
                            send_port = ports[worker_index]
                            send_host = hosts[worker_index]
                            sql = in_msg.format(row.__repr__())
                            
                            # if data for ourself, just do the sql
                            if send_port == port and send_host == hostname:
                                 if DEBUG >= 2:
                                      print(sql)
                                 cursor = self.cnx.cursor()
                                 cursor.execute(sql)
                                 cursor.close()
                            else:
                                 if wsockets[worker_index] is None:
                                     wsockets[worker_index] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                     wsockets[worker_index].connect((send_host, send_port))
                                 self.send(wsockets[worker_index], sql)
                                 if self.recv(wsockets[worker_index]) != 'OK':
                                     print("Oops. An error during shuffle.")
                                     raise Exception("Oops. An error during shuffle.")
                        out_msg="OK"
                        if DEBUG >= 1:
                             print(len(self.mapresult),"rows have been shuffled")
                        self.cnx.commit()
                        self.mapresult=None
                    finally:
                        for s in wsockets:
                            if s is not None:
                                 s.close()
                    
                elif cmd == "reduce":
                    # execute the sql select 
                    # return result set to client
                    sql = in_msg[ind:].strip()
                    out_msg = self.do_select(sql)
                    
                    
                elif cmd == "select":
                    sql = in_msg 
                    out_msg = self.do_select(sql)
                
                else:  
                    # process a non-select sql statement .
                    # no result set to send to client.
                    sql = in_msg 
                    if DEBUG >=2:
                         print(sql)
                    cursor = self.cnx.cursor()
                    cursor.execute(sql)
                    self.cnx.commit()
                    out_msg="OK"
                    
                # send reply back to client
                if self.send(self.clientsocket, out_msg) == -1:
                    return 
                
        except:
             print("Oops.  Something unexpected.", sys.exc_info()[0])
             traceback.print_exc(file=sys.stdout)
             
        finally:
             self.cnx.close()
             self.clientsocket.close()
             
             
    def do_select(self, sql):
        if DEBUG >=2:
             print(sql)
        cursor = self.cnx.cursor()
        cursor.execute(sql)
        out_msg=' '
        row = cursor.fetchone()
        while row is not None:
            out_msg=out_msg + row.__repr__()
            row = cursor.fetchone()
        self.cnx.commit()
        return out_msg
        
        
    def recv(self,sock):
        buffer = b''
        while True:
            chunk = sock.recv(2048)
            if len(chunk)==0:
                rc = -1
                break
            else:
                buffer = buffer+chunk
                if buffer[-1]==0:
                   rc =  buffer[0:-1].decode('utf-8')
                   break
                   
        if DEBUG >=2: 
             print("sent","msg=",rc)
        return rc
            

    def send(self, sock, msg):
        buffer = bytes(msg,'utf-8')+b'\x00'
        buflen = len(buffer)
        start = 0
        while start < buflen: 
            sentbytes = sock.send(buffer[start:])
            if sentbytes==0:
                if DEBUG >= 2:
                    print("sent", "Client has gone away")
                return -1
            start=start+sentbytes
        if DEBUG >=2: 
             print("sent","msg=",msg)
        return 0
        
  
def readConfig():
    global USERID, PASSWORD, hosts, ports, DEBUG 
    f = open("config.txt")
    for line in f:
       tokens=line.split()
       if tokens[0]=='userid':
           USERID = tokens[1]
       elif tokens[0]=='password':
           PASSWORD = tokens[1]
       elif tokens[0]=='worker':
           hosts.append(tokens[1])
           ports.append(int(tokens[2]))
       elif tokens[0]=='debug':
           DEBUG = int(tokens[1])
       else:
           print("configuration file error", line)
    f.close()
  

# main worker program 

readConfig()
if port not in ports:
   print("Error.  port number must be one of ",ports)
   raise Exception("Bad port number")
   
# create an INET, STREAMing socket
serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# bind the socket to a host and port number
serversocket.bind((hostname, port))
# become a server socket
serversocket.listen(5)
print("Worker listening:", hostname, port)

while True:
    (clientsocket, address) = serversocket.accept()           # accept connection from outside
    if DEBUG >= 2:
         print("Worker", port, "accepted incoming connection")
    ct = Worker(clientsocket)    # create background task to handle request
    ct.start()                   # start task
	
