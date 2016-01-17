#!/usr/bin/python

import datetime
import os
import smtplib
import subprocess
from ConfigParser import SafeConfigParser

# Import the email modules
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

current_time = datetime.datetime.now()
apps = []
out = ''
err = ''
proc = ''
isLongrunning = 0
noOfWaitingJob = 0
runnning_jobs = []
accepted_jobs = []
content = ''
RMUrl = 'http://localhost:8088/cluster'
longrunningThreshold = 30
Subject = 'Hadoop Cluster - Long running Hadoop MR jobs '
FROM = 'from@mail.com '
TO = 'to@mail.com '
SMTPServer = 'gmail.com'


def app_time(t):
    return datetime.datetime.fromtimestamp(int(t) / 1e3)


def hours_elapsed(t):
    return (current_time - app_time(t))


# For Running applications
def running_apps(appid):
    command = "yarn application -status " + appid
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    lines = out.split("\n")
    app_time = datetime.datetime.fromtimestamp(int(lines[6].split(":")[1]) / 1e3)
    runnning_jobs.append(lines[1].split(":")[1] + "," + lines[2].split(" :")[1] + "," + lines[4].split(":")[1] + "," + \
                         lines[6].split(":")[1] + "," + lines[8].split(":")[1] + "," + lines[9].split(":")[1] + "," +
                         lines[11].split(" :")[1])
    # print  lines[1].split(":")[1], lines[2].split(":")[1], lines[4].split(":")[1], lines[6].split(":")[1], \
    #    lines[8].split(":")[1], lines[9].split(":")[1], app_time, (current_time - app_time)



def accepted_apps(appid):
    global noOfWaitingJob
    try:
        command = "yarn application -status " + appid
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        lines = out.split("\n")
        app_time = datetime.datetime.fromtimestamp(int(lines[6].split(":")[1]) / 1e3)
        accepted_jobs.append(
            lines[1].split(":")[1] + "," + lines[2].split(" :")[1] + "," + lines[4].split(":")[1] + "," + \
            lines[6].split(":")[1] + "," + lines[8].split(":")[1] + "," + lines[9].split(":")[1] + "," +
            lines[11].split(" :")[1])
        noOfWaitingJob += 1
        # print  lines[1].split(":")[1], lines[2].split(":")[1], lines[4].split(":")[1], lines[6].split(":")[1], \
        #    lines[8].split(":")[1], lines[9].split(":")[1], app_time, (current_time - app_time)
    except Exception, e:
        print ("Exception error: executing yarn application: " + e)


def headerHTML():
    global noOfWaitingJob
    global isLongrunning
    s = ''
    s += '<html> '
    s += '<head> '
    s += '<style> '
    dir = os.path.dirname(__file__)
    filename = os.path.join(dir, './yarn.css')
    # print filename
    file = open(filename, 'r')
    s += file.read()
    s += '</style>'
    s += '<meta charset=ISO-8859-1>'
    s += '<title>Hadoop Platform</title>'
    s += '<h2> Long running Hadoop Mapreduce Jobs  </h2>'
    s += '<h4>Date of Snapshot:  ' + current_time.strftime('%Y-%m-%d %H:%M:%S') + '</h4>'
    s += 'In BDL YARN cluster, bellow job(s) running more than ' + str(longrunningThreshold) + ' min limt, and ' + str(
        noOfWaitingJob) + ' jobs waiting for cluster resource'
    s += '</head>'
    s += '<body>'
    return s


def footerHTML():
    s = "<br>"
    s += "<h4> This mail is triggered, because BDL Produciton Hadoop MR jobs are running more than " + str(
        longrunningThreshold) + " mins, </h4>"
    s += "<h4> Please check the YARN <a href= " + RMUrl + "> Resource Manager </a>. </h4> "
    s += '</body>'
    s += "</html>"
    return s


def acceptedJobs2HTML():
    global noOfWaitingJob
    s = '<br>'
    s += '<h2> ' + str(noOfWaitingJob) + ' jobs are waiting in the YARN Resource Manager </h2>'
    s += '  <table> '
    s += '      <thead> '
    s += '        <tr> '
    s += '          <th>Application ID </th> '
    s += '          <th>Started Time</th> '
    s += '          <th>Awaiting Time (mins)</th> '
    s += '          <th>Hadoop Job Name</th> '
    s += '          <th>Hadoop User</th> '
    s += '          <th>Percentage of Complete</th> '
    s += '        </tr> '
    s += '      </thead> '

    for i in range(len(accepted_jobs)):
        lines = accepted_jobs[i].split(",")
        s += '     <tr> '
        s += '         <td>' + lines[0] + '</td>'
        s += '         <td>' + app_time(lines[3]).strftime("%Y-%m-%d %H:%M") + '</td>'
        s += '         <td>' + str(hours_elapsed(lines[3]).seconds / 60) + '</td>'
        s += '         <td>' + lines[1] + '</td>'
        s += '         <td>' + lines[2] + '</td>'
        s += '         <td>' + lines[4] + '</td>'
        s += '     </tr> '
    s += '  </table> '
    return s


def runningJobs2HTML():
    global isLongrunning
    global longrunningThreshold

    s = '<br>'
    s += '<h2> Long running Jobs in the YARN Resource Manager </h2>'
    s += '  <table> '
    s += '      <thead> '
    s += '        <tr> '
    s += '          <th>Application ID </th> '
    s += '          <th>Started Time</th> '
    s += '          <th>Elapsed Time (mins)</th> '
    s += '          <th>Hadoop Job</th> '
    s += '          <th>Hadoop User</th> '
    s += '          <th>Percentage of Complete</th> '
    s += '        </tr> '
    s += '      </thead> '

    for i in range(len(runnning_jobs)):
        lines = runnning_jobs[i].split(",")
        s += '     <tr> '
        s += '          <td> <a href=' + lines[6] + '>' + lines[0] + '</a></td>'
        s += '         <td>' + app_time(lines[3]).strftime("%Y-%m-%d %H:%M") + '</td>'
        s += '         <td>' + str(hours_elapsed(lines[3]).seconds / 60) + '</td>'

        if int(hours_elapsed(lines[3]).seconds / 60) > int(longrunningThreshold):
            isLongrunning += 1
        s += '         <td>' + lines[1] + '</td>'
        s += '         <td>' + lines[2] + '</td>'
        s += '         <td>' + lines[4] + '</td>'
        s += '     </tr> '
    s += '  </table> '
    return s


def read_properties():
    global RMUrl
    global longrunningThreshold
    global Subject
    global FROM
    global TO
    global SMTPServer
    dir = os.path.dirname(__file__)
    filename = os.path.join(dir, './config.ini')
    parser = SafeConfigParser()
    parser.read(filename)

    RMUrl = parser.get('script', 'RMUrl')
    longrunningThreshold = parser.get('script', 'longrunningThreshold')
    FROM = parser.get('script', 'FROM')
    TO = parser.get('script', 'TO')
    Subject = parser.get('script', 'Subject')
    SMTPServer = parser.get('script', 'SMTPServer')


def sendMail(s):
    global TO
    global FROM
    global Subject
    global SMTPServer

    msg = MIMEMultipart('alternative')
    msg['To'] = TO
    msg['From'] = FROM
    msg['Subject'] = Subject
    part = MIMEText(s, 'html')
    msg.attach(part)

    # print s

    try:
        smtpObj = smtplib.SMTP(SMTPServer)
        smtpObj.sendmail(FROM, TO, msg.as_string())
        print "Successfully sent email"
        smtpObj.quit()
    except Exception, e:
        print "Error: unable to send email", e


def main():
    try:
        proc = subprocess.Popen("yarn application -list --appStates RUNNING,ACCEPTED", shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
    except Exception, e:
        print ("Exception error: executing yarn application: " + e)

    apps = out.split("\n")

    app = []
    for i in range(2, len(apps) - 1):
        app = apps[i].split("\t")
        if app[5].strip() == "RUNNING":
            print app[0]
            running_apps(app[0])
        elif app[5].strip() == "ACCEPTED":
            print app[0]
            accepted_apps(app[0])
    print runnning_jobs
    print accepted_jobs
    content = ""
    content += headerHTML()
    content += runningJobs2HTML()
    content += acceptedJobs2HTML()
    content += footerHTML()
    return content

if __name__ == '__main__':
    read_properties()
    s = main()
    print s
    print isLongrunning
    if isLongrunning > 0:
        sendMail(s)
