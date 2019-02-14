# HRclient.py
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


 
#  main 

readConfig()

# create test data and write to employee.data, department.data and manager.data
file_emp = open("employee.data", "w")
file_dept = open("department.data", "w")
file_mgr = open("manager.data", "w")
# create 1000 departments and employees who are managers of the departments
for dept in range(1, 1001):
   empid = 2*dept
   name = "John Manager"+str(dept)
   dept_name = "Dept #"+str(dept)
   salary = random.randint(100000, 150000)
   file_emp.write(str(empid)+', "'+name+'", '+str(dept)+', '+str(salary)+'\n')
   file_dept.write(str(dept)+', "'+dept_name+'"\n')
   file_mgr.write(str(empid)+', '+str(dept)+'\n')
# now generate 50000 other employees and assign to random departments
for empid in range(3000, 53000):
   name = "Joe Employee"+str(empid)
   dept = random.randint(1,1000)
   salary = random.randint(60000, 250000)
   file_emp.write(str(empid)+', "'+name+'", '+str(dept)+', '+str(salary)+'\n')
file_emp.close()
file_dept.close()
file_mgr.close()
   

c = Coordinator()
c.sendToAll("drop table if exists employee")
c.sendToAll("drop table if exists manager")
c.sendToAll("drop table if exists department")
c.sendToAll("create table employee (empid int primary key, name char(20), dept int, salary double)") 
c.sendToAll("create table manager (mgr_id int primary key, dept int)")
c.sendToAll("create table department (dept int primary key, dept_name char(20))")
print("Loading employee table.  This will take a few minutes.")
c.loadTable("employee", "employee.data")
print("Loading manager table")
c.loadTable("manager", "manager.data")
print("Loading department table")
c.loadTable("department", "department.data")
c.close()
