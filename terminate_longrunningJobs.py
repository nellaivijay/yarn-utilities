#!/usr/bin/python

import datetime
import logging
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
noOfRunningJob = 0
noOfWaitingJob = 0
runnning_jobs = {}
accepted_jobs = {}
content = ''
RMUrl = 'http://localhost:8088/cluster'
terminationThreshold = 90
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
    global noOfRunningJob
    command = "yarn application -status " + appid
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    lines = out.split("\n")
    runnning_jobs[lines[1].split(":")[1]] = int(lines[6].split(":")[1])
    noOfRunningJob += 1
    # print  lines[1].split(":")[1], lines[2].split(":")[1], lines[4].split(":")[1], lines[6].split(":")[1], \
    #    lines[8].split(":")[1], lines[9].split(":")[1], app_time, (current_time - app_time)


def accepted_apps(appid):
    global noOfWaitingJob
    try:
        command = "yarn application -status " + appid
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        lines = out.split("\n")
        accepted_jobs[lines[1].split(":")[1]] = int(lines[6].split(":")[1])
        noOfWaitingJob += 1
        # print  lines[1].split(":")[1], lines[2].split(":")[1], lines[4].split(":")[1], lines[6].split(":")[1], \
        #    lines[8].split(":")[1], lines[9].split(":")[1], app_time, (current_time - app_time)
    except Exception, e:
        print ("Exception error: executing yarn application: " + e)


def reportHTML(appid):
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
    s += '<h2> Long running Hadoop Mapreduce Job ' + appid + ', terminated by Self Healing process  </h2>'
    s += '<h4> TimeStamp :  ' + current_time.strftime('%Y-%m-%d %H:%M:%S') + '</h4>'
    s += 'In Hadoop YARN cluster, bellow job running more than ' + str(
        terminationThreshold) + ' min limt, which was reached the self healing limiitation'
    s += '</head>'
    s += '<body>'
    command = "yarn application -status " + appid
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    lines = out.split("\n")
    s += ' <br> <table> '
    s += '      <thead> '
    s += '        <tr> '
    s += '          <th> </th> '
    s += '          <th> </th> '
    s += '        </tr> '
    s += '      </thead> '
    s += '      <tr> <td> Applicaiton Id </td> <td> <a href=' + lines[11].split(" :")[1] + '>' + lines[1].split(":")[
        1] + '</a></td></tr>'
    s += '      <tr> <td> Application-Name </td> <td> ' + lines[2].split(":")[1] + '</td></tr>'
    s += '      <tr> <td> User </td> <td> ' + lines[4].split(":")[1] + '</td></tr>'
    s += '      <tr> <td> Resource Queue </td> <td> ' + lines[5].split(":")[1] + '</td></tr>'
    s += '      <tr> <td> Start Time </td> <td> ' + app_time(lines[6].split(":")[1]).strftime(
        "%Y-%m-%d %H:%M") + '</td></tr>'
    s += '      <tr> <td> Finesh Time </td> <td> ' + app_time(lines[7].split(":")[1]).strftime(
        "%Y-%m-%d %H:%M") + '</td></tr>'
    s += '      <tr> <td> Elapsed Time  </td> <td> ' + str(
        hours_elapsed(lines[6].split(":")[1]).seconds / 60) + ' mins </td></tr>'
    s += '      <tr> <td> State </td> <td> ' + lines[9].split(":")[1] + '</td></tr>'
    s += ' </table> <br>'
    s += "<h4> This mail is triggered by Self Healing process </h4> "
    s += "<h4> Please check the YARN <a href= " + RMUrl + "> Resource Manager </a>. </h4> "
    s += '</body>'
    s += "</html>"
    return s


def kill_apps(appid):
    logging.debug("KILLING application %s" % appid)
    subprocess.call("yarn application -kill %s" % appid, shell=True)


def main():
    s = ""
    global isLongrunning
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
    killAppID = ''
    print 'Running Jobs ' + str(noOfRunningJob)
    for key, value in sorted(runnning_jobs.iteritems(), key=lambda item: item[1], reverse=False)[:1]:
        print "%s: %s %s %s" % (key, value, hours_elapsed(value).seconds / 60, terminationThreshold)
        if int(hours_elapsed(value).seconds / 60) > int(terminationThreshold):
            print (hours_elapsed(value).seconds / 60)
            isLongrunning += 1
            killAppID = key
    print isLongrunning, killAppID, terminationThreshold
    if isLongrunning > 0:
        print  killAppID, isLongrunning
        kill_apps(killAppID)
        s = reportHTML(killAppID)
    return s


def read_properties():
    global RMUrl
    global terminationThreshold
    global Subject
    global FROM
    global TO
    global SMTPServer
    dir = os.path.dirname(__file__)
    filename = os.path.join(dir, './config.ini')
    parser = SafeConfigParser()
    parser.read(filename)

    RMUrl = parser.get('script', 'RMUrl')
    terminationThreshold = parser.get('script', 'terminationThreshold')
    FROM = parser.get('script', 'FROM')
    TO = parser.get('script', 'TO')
    SMTPServer = parser.get('script', 'SMTPServer')


def sendMail(s):
    global TO
    global FROM
    global Subject
    global SMTPServer

    msg = MIMEMultipart('alternative')
    msg['To'] = TO
    msg['From'] = FROM
    msg['Subject'] = 'BDL Hadoop Self healing process was terminated a blocking Hadoop MR Job'
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


if __name__ == '__main__':
    read_properties()
    s = main()
    print s
    print isLongrunning
    if isLongrunning > 0:
        sendMail(s)
