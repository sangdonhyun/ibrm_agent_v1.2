#!/bin/sh
echo `netstat -an | grep 53001`
export PATH=/iBRM/ibrm_agent_v1.2/python27/bin:$PATH
echo $PATH
if [[ $(id -u) -ne 0 ]] ; then echo "Please run as root" ; exit 1 ; fi
which pythonk
python ./ibrm_menu.py