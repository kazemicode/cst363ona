# coordinator.py
import socket, random

USERID=''
PASSWORD=''
hosts=[]
ports=[]
DEBUG = 1

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

class Coordinator:
    def __init__(self):
       self.sockets = []     
       # connect to all workers
       for hostname, port in zip(hosts, ports):
           sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
           sock.connect((hostname, port))
           self.sockets.append(sock)
           if DEBUG >=2:
                print("Connected to", hostname, port)
       
    
    def close(self):
        for sock in self.sockets:
           sock.close();
        self.sockets = []
        if DEBUG >= 2:
             print("All worker connections closed")
    
    def sendToAll(self, stmt):
        for sock,port in zip(self.sockets,ports):
           self.send(sock, stmt)
           if DEBUG >= 1:
               print("to port=", port, "msg=", stmt)
               
        for sock,port in zip(self.sockets,ports):
           status_msg = self.recv(sock)
           if DEBUG >= 1:
               print("from port=",port,"msg=",status_msg)
               
           # check if message received is a set of rows
           # if yes, then print one row per line
           status_msg = str(status_msg).strip()
           if status_msg.startswith("("):
               index = 0
               while index < len(status_msg) and status_msg[index]=='(':
                   index_next = status_msg.find(')',index)
                   print(status_msg[index+1:index_next])
                   index = index_next+1
   
    def recv(self,sock):
        buffer = b''
        while True:
            chunk = sock.recv(2048)
            if len(chunk)==0:
                print('connection error', sock)
                return -1
            buffer = buffer+chunk
            if buffer[-1]==0:
                return buffer[0:-1].decode('utf-8')
            
        
    def send(self, sock, msg):
        buffer = bytes(msg,'utf-8')+b'\x00'
        buflen = len(buffer)
        start = 0
        while start < buflen: 
            sentbytes = sock.send(buffer[start:])
            if sentbytes==0:
                print("connection error", sock)
                return -1
            start=start+sentbytes
        if DEBUG >= 1:
             print("send msg=",msg)
        return 0
    
    def loadTable(self, tableName, filename):
        # read file of data values which must be comma separated and in the same column order as the schema columns
        # first column is the key (which must be integer) and is hashed to distributed the data across
        # worker nodes
        f = open(filename, 'r')
        for line in f:
            line = line.strip()
            sql='insert into '+tableName+' values('+line+')'
            intkey= line[0: line.index(',')]
            index = hash((int(intkey)))%len(ports)
            # index is which server to get the data
            self.send(self.sockets[index], sql)
            rc = self.recv(self.sockets[index])
            if DEBUG >= 1:
                 print("sent",sql,"received",rc)
        f.close()
          
    
    def getRowByKey(self, sql, key):
        index = hash(key)%len(ports)
        self.send(self.sockets[index], sql)
        rc = self.recv(self.sockets[index])
        print("getRowByKey data=", rc)

    
    # def getRowByNonKey(self, tableName, whereClause):
        # sql = 'select * from '+tableName+' where '+whereClause
        # print("getRowByNonKey", tableName, whereClause)
        # for sock in self.sockets:
           # self.send(sock,sql)
           # status_msg=self.recv(sock)
           # if DEBUG >= 1:
                # print("send", sql, "received",status_msg)
           # status_msg = status_msg.strip()  # remove whitespace from front and endswith
           # index = 0
           # while index < len(status_msg) and status_msg[index]=='(':
                # index_next = status_msg.find(')',index)
                # print(status_msg[index+1:index_next])
                # index = index_next+1
        # print("getRowByNonKey end")
           
 
#  main 

readConfig()

# create test data and write to emp.data file
# f = open("emp.data", "w")
# for empid in range(1,100):
    # dept = random.randint(100, 105);
    # salary = random.randint(90000, 250000)
    # name = 'Joe Employee'+str(empid)
    # line = str(empid)+', "'+name+'", '+str(dept)+', '+str(salary)+'\n'
    # f.write(line)
# f.close()

c = Coordinator()
c.sendToAll("drop table if exists emp")
c.sendToAll("create table emp (empid int primary key, name char(20), dept int, salary double)") 
c.loadTable("emp", "emp.data")

# example of find by key
for empid in range(1,5):
    c.getRowByKey("select * from emp where empid="+str(empid), empid)
    
# example of find by non key
# c.getRowByNonKey("emp", "dept=100 or salary > 100000 or name like '%7' ")
c.sendToAll("reduce select * from emp where dept=100 or salary > 100000 or name like '%7'")


# example of map-shuffle-reduce

# crate temp table
c.sendToAll("drop table if exists tempdept ")
c.sendToAll("create table tempdept (dept int, salary double)")

# map phase
# map phase executes the select but holds the result data at the worker
# map should be followed by shuffle
c.sendToAll("map select dept, salary from emp ")

# shuffle phase
# the result from prior map phase is distributed to servers
# and inserted into temp table.
# use {} as place holder for data values as shown below.
c.sendToAll("shuffle insert into tempdept  values {}") 

# reduce phase
# execute the select and return result to client.
print("Reduce result set")
c.sendToAll("reduce select dept, avg(salary), count(*) from tempdept  group by dept order by dept")

# clean up - delete temp table
c.sendToAll("drop table if exists tempdept ")

c.close()
