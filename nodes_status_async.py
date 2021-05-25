#!/home/kjgong/anaconda3/bin/python3
import sys
from collections import defaultdict
import asyncio 
from concurrent.futures import ThreadPoolExecutor

import paramiko
import arrow
from prettytable import PrettyTable

STATUS = defaultdict(dict)

class Printer:
    def __init__(self,nodes):
        self.nodes = nodes
        self.height = len(self.nodes)+4

    def built_table(self):
        x = PrettyTable(["Node", "CPU usage", "Last used", "User name","Task"])
        x.float_format = "2.2"
        for node in self.nodes:
            status = STATUS[node]
            x.add_row([
                node,
                status.get("cpu",""),
                status.get("last",""),
                status.get("user",""),
                status.get("task",""),
                ],)

        return x.get_string()
    
    def print_placeholder(self):
        sys.stdout.write("\n"*self.height)
        sys.stdout.flush()

    def display(self):
        table_str = self.built_table()
        sys.stdout.write("\033[F"*self.height)
        sys.stdout.write(table_str+"\n")
        sys.stdout.flush()

class SSHclient(object):
    def __init__(self,node,printer):
        self.node = node
        self.p = printer
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.node,banner_timeout=200)
        self.client.exec_command("export LANG=en_US")

    def run(self):
        STATUS[self.node]["cpu"] = self.get_cpu_usage()
        self.p.display()
        STATUS[self.node]["last"] = self.get_last_use_time()
        self.p.display()
        STATUS[self.node]["user"],STATUS[self.node]["task"] = self.get_user_and_task()
        self.p.display()
        self.client.close()

    def get_cpu_usage(self):
        ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command("sar 1 1  | tail -1 | awk '{print $NF}'")
        return 100 - float(ssh_stdout.read())

    def get_last_use_time(self):
        if STATUS[self.node]["cpu"]>50:
            return "Now"
        now = arrow.now()
        for i in range(10):
            date = now.shift(days=-i)
            COMMAND = f"sar -f /var/log/sa/sa{date.day:02d} | grep M | grep -v CPU | awk '{{if ($NF<0.5) print $1}}' |tail -1"
            ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command(COMMAND)
            if ssh_stderr.readlines():
                continue
            row = ssh_stdout.read()
            if row:
                last = arrow.get(f"{date.format('MM/DD/YYYY')} {row}","MM/DD/YYYY HH:mm:ss A",tzinfo="+08:00")
                return last.humanize()
        else:
            return date.humanize()
    def get_user_and_task(self):
        COMMAND = "ps aux | grep -v root | awk '{if($3>30) print $1,$11}'|uniq -c |sort -rn |head -1"
        ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command(COMMAND)
        row = ssh_stdout.read().decode("utf-8")
        if row:
            return row.split()[1],row.split()[-1].split("/")[-1]
        else:
            return [""]*2

def main(node):
    c = SSHclient(node,p)
    c.run()

if __name__ == "__main__":
    nodes = [f"n{i:03d}" for i in range(1,27)] 
    if len(sys.argv) > 1:
        nodes = set(nodes) & sys.argv[1].split(",")
    
    p = Printer(nodes)
    p.print_placeholder()
    p.display() 
    executor = ThreadPoolExecutor(len(nodes))
    executor.map(main,nodes)
