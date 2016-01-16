#!/usr/bin/python

import subprocess

apps = []
out = ""
err = ""
proc = ""


def running_apps(appid):
    command = "yarn application -status " + appid
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    lines = out.split("\n")
    print lines[1], lines[1].split(":")[1], , lines[2].split(":")[1], , lines[4].split(":")[1], lines[5].split(":")[1], \
    lines[7].split(":")[1], lines[9].split(":")[1]


def accepted_apps(appid):
    command = "yarn application -status " + appid
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    print out

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
            print app[0], app[1], app[3], app[5], app[7]
            running_apps(app[0])
        elif app[5].strip() == "ACCEPTED":
            print app[0], app[1], app[3], app[5], app[7]
            accepted_apps(app[0])


if __name__ == '__main__':
    main()
