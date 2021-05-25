#!/home/kjgong/anaconda3/bin/python3
import time
import os
import sys
import threading
import paramiko
import arrow
# from multiprocessing.dummy import Pool
import queue



from prettytable import PrettyTable

ts =  os.get_terminal_size()
if ts.lines < 31:
    exit("ERROR:Your terminal size is too small to print this table!")


global_res = {f"n{node:03d}":["","","",""] for node in range(1,27)}
end = {f"n{node:03d}":False for node in range(1,27)}

class SSHclient(object):
    def __init__(self,node_name):
        self.node_name = node_name
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.node_name)
        self.client.exec_command("export LANG=en_US")
        
    def get_cpu_usage(self):
        ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command('sar 1 1')
        # print(ssh_stdout.readlines())
        global_res[self.node_name][0] = 100 - float(ssh_stdout.readlines()[-2].split()[-1])

    def get_last_use(self):
        if global_res[self.node_name][0] > 50 :
            global_res[self.node_name][1] = "Now"
        else:
            for i in range(10):
                d = arrow.now().shift(days=-i).day
                ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command(f"sar -f /var/log/sa/sa{d:02d}")
                if ssh_stderr.readlines():
                    continue
                row = ssh_stdout.readlines()
                date = row[0].split("\t")[1]
                # print(f"date=>{date}")
                for r in reversed(row):
                    if "CPU" in r or "Average" in r:
                        continue
                    usage = r.split()
                    if len(usage)==9:
                        if float(usage[3]) > 50:
                            time = arrow.get(f"{date}{usage[0]} {usage[1]}","MM/DD/YYYY HH:mm:ss A",tzinfo="+08:00")
                            # global_res[self.node_name][1] = f"{date}{usage[0]} {usage[1]}"
                            global_res[self.node_name][1] =  time.humanize()
                            return
            else:
                time = arrow.get(f"{date}","MM/DD/YYYY",tzinfo="+08:00")
                global_res[self.node_name][1] =  time.humanize()
    def get_user_name(self):
        if global_res[self.node_name][0] < 30:
            return 
        ssh_stdin, ssh_stdout, ssh_stderr  = self.client.exec_command("ps aux | grep -v root | grep -v grep | awk '{if($3>30) print $1,$11}'|uniq -c |sort -rn |head -1")
        if ssh_stderr.readlines():
            return 
        stdout = ssh_stdout.readlines()
        if not stdout:
            return
        else:
            row = stdout[0].split()
            global_res[self.node_name][2] = row[1]
            global_res[self.node_name][3] = row[2].split("/")[-1][:36]


    def close(self):
        self.client.close()

def main():
    # os.system("clear")
#    print("\n"*20)
    sys.stdout.write("\n"*30)
    # exit()
    while not all(end.values()):
        # print(end)
        time.sleep(0.25)
        # print(global_res)
        x = PrettyTable(["Node", "CPU usage", "Last used", "User name","Task"])
        x.float_format = "2.2"
        for k,v in global_res.items():
            x.add_row([k]+v)
        x = x.get_string()
        # os.system("clear")
        # print(x,flush=True)
        sys.stdout.write("\033[F"*30)
        sys.stdout.write(x+"\n")
        #sys.stdout.write("\n")
        sys.stdout.flush()

def update():
    # print(node)
    # print( Q.qsize())
    while Q.qsize():
        node = Q.get()
        try:
            s = SSHclient(node)
            s.get_cpu_usage()
            s.get_last_use()
            s.get_user_name()
            s.close()
            global end
            end[node] = True
        except:
            #global_res[node]=["*"]*4 
            end[node] = True
            pass

    # print(s.get_last_use())

if __name__ == "__main__":
    try:
        pool = []
        Q = queue.Queue()
        for i in range(1,27):
            Q.put(f"n{i:03d}")

        for i in range(26):
            t = threading.Thread(target=update)
            t.start()
            pool.append(t)

        t = threading.Thread(target=main)
        t.start()
        pool.append(t)

        [t.join() for t in pool]
    except :
        exit()
    # [t.kill() for t in pool]

