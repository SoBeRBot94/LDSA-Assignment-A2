#!/usr/bin/env python3

# Python Fabric File To Deploy Spark in Standalone Mode

from fabric.api import *

env.user = 'ubuntu'
env.key_filename = '/home/SoBeRBot94/.ssh/soberbot94.pem'

with open("hostfile") as hf:
    for i, line in enumerate(hf):
        if i == 0:
            env.hosts = line
        elif i == 1:
            workerFIP = line
        elif i == 2:
            masterIPv4 = line

@task
def set_hostname():
    print("\n \n ----- Set Hostname ----- \n \n")
    sudo('hostnamectl set-hostname spark-master-only')
    sudo('systemctl restart systemd-hostnamed')
    sudo('echo \'spark-master-only\' > /etc/hostname')
    sudo('sed -i \'s/127.0.0.1 localhost.*$/127.0.0.1 localhost spark-master-only/\' /etc/hosts')

@task
def install_updates():
    print("\n \n ----- Installing Updates ----- \n \n")
    sudo('apt-get -y update')

@task
def install_requisites():
    print("\n \n ----- Installing Requisites ----- \n \n")
    sudo('apt-get -y install default-jre scala python3 python3-pip supervisor')

@task
def upgrade_pip():
    print("\n \n ----- Upgrading PIP Version ----- \n \n")
    sudo('python3 -m pip install --upgrade pip')

@task
def install_jupyter():
    print("\n \n ----- Install Jupyter ----- \n \n")
    sudo('python3 -m pip install jupyter')

@task
def setup_jupyter_service():
    print("\n \n ----- Setting up Supervisor Service Configuration For Jupyter ----- \n \n")
    put('./jupyter.conf', '/etc/supervisor/conf.d/jupyter.conf', use_sudo=True)
    sudo('supervisorctl reread')
    sudo('supervisorctl reload')

@task
def install_pyspark():
    print("\n \n ----- Installing PySpark ----- \n \n")
    sudo('python3 -m pip install pyspark')

@task
def setup_pyspark_env():
    print("\n \n ----- Setup PySpark Environment ----- \n \n")
    sudo('echo >> /etc/profile')
    sudo('echo \'export PYSPARK_PYTHON=/usr/bin/python3\' >> /etc/profile')
    sudo('echo \'export PYSPARK_DRIVER_PYTHON=/usr/bin/ipython\' >> /etc/profile')

@task
def set_java_env():
    print("\n \n ----- Setting Java Environment Variables ----- \n \n")
    sudo('echo >> /etc/profile')
    sudo('echo \'export JAVA_HOME=/usr/lib/jvm/default-java\' >> /etc/profile')
    sudo('echo \'export PATH=$PATH:$JAVA_HOME/bin\' >> /etc/profile')

@task
def fetch_spark_tarball():
    print("\n \n ----- Fetching Spark Tar Ball ----- \n \n")
    sudo('wget http://www-eu.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz')

@task
def extract_spark_tarball():
    print("\n \n ----- Extract ----- \n \n")
    sudo("tar xvf spark-2.3.0-bin-hadoop2.7.tgz")

@task
def setup_spark_env():
    print("\n \n ----- Setting Up Spark Environment & Environment variables ----- \n \n")
    sudo('mv /home/ubuntu/spark-2.3.0-bin-hadoop2.7 /usr/local/spark')
    sudo('chown -R ubuntu:ubuntu /usr/local/spark')
    sudo('echo >> /etc/profile')
    sudo('echo \'export SPARK_HOME=/usr/local/spark\' >> /etc/profile')
    sudo('echo \'export PATH=$PATH:$SPARK_HOME/bin\' >> /etc/profile')

@task
def configure_spark():
    print("\n \n ----- Configure Spark Master Node ----- \n \n")
    run('cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh')
    run('echo >> /usr/local/spark/conf/spark-env.sh')
    run('echo \'export JAVA_HOME=/usr/lib/jvm/default-java\' >> /usr/local/spark/conf/spark-env.sh')
    run('echo \'export SPARK_WORKER_CORES=6\' >> /usr/local/spark/conf/spark-env.sh')
    run('echo \'export SPARK_MASTER_HOST=%s\' >> /usr/local/spark/conf/spark-env.sh' % masterIPv4)
    run('echo \'%s\' > /usr/local/spark/conf/slaves' % workerFIP)

@task
def clean_up():
    print("\n \n ----- Cleaning Up Junk ----- \n \n")
    local('rm -rf ./__pycache__')

@task
def auto_deploy():
    set_hostname()
    install_updates()
    install_requisites()
    upgrade_pip()
    install_jupyter()
    setup_jupyter_service()
    install_pyspark()
    setup_pyspark_env()
    set_java_env()
    fetch_spark_tarball()
    extract_spark_tarball()
    setup_spark_env()
    configure_spark()
    clean_up()
