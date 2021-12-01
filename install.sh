#!/bin/bash
export IBRM_PATH='/IBRM_TEST'
if [ ! -d ${IBRM_PATH} ] ; then
 mkdir ${IBRM_PATH}
fi
export INST_DIR=`pwd`
alias cp='cp -i'
cp ./ibrm_agent_v1.tar ${IBRM_PATH}
cd ${IBRM_PATH}
tar -xvf ibrm_agent_v1.tar
rm -rf ${IBRM_PATH}/ibrm_agent_v1.tar
cd ibrm_agent_v1
export PATH=${IBRM_PATH}/python27/bin:$PATH
./python27/bin/python install.py $INST_DIR $IBRM_PATH
sh ./ibrm_start.sh