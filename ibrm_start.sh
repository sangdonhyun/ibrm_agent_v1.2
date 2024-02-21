. /product/Fleta_Workspace/ibrm_agent/Profile
#if [[ $(id -u) -ne 0 ]] ; then echo 'Please run as root' ; exit 1 ; fi
export PATH=/product/Fleta_Workspace/ibrm_agent_ora/python27/bin:$PATH

echo `pwd`
echo `which python`
cd /product/Fleta_Workspace/ibrm_agent_ora

python ./ibrm_menu.py
