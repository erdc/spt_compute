
#*******************************************************************************
#install_rapid_htcondor.sh
#*******************************************************************************

#Purpose:
#This script installs programs required for RAPID and HTCONDOR
#Authors:
#Alan D. Snow & Scott D. Christensen, 2015
#USE AT YOUR OWN RISK!!!!!!!

#*******************************************************************************
#Instructions
#*******************************************************************************
#Make sure you give this file execute privelidges
#And, change the NAME variable to your username
NAME="alan"

#*******************************************************************************
# Main Code
#*******************************************************************************
#Install Prereqs
sudo apt-get install gfortran g++ python-pip python-dev zlib1g-dev libhdf5-serial-dev libnetcdf-dev 
pip install numpy
pip install netCDF4 requests_toolbelt condorpy
pip install tethys_dataset_services
sudo apt-get install git
cd /home/$NAME/
mkdir condor ecmwf logs scripts rapid rapid/input rapid/output
cd scripts
git clone https://github.com/CI-WATER/erfp_data_process_ubuntu_aws.git
cd erfp_data_process_ubuntu_aws
git submodule init
git submodule update
#install RAPID prereqs
cd /home/$NAME/
mkdir installz work
cd installz
wget "http://ftp.mcs.anl.gov/pub/petsc/release-snapshots/petsc-3.3-p7.tar.gz"
wget "http://www.mcs.anl.gov/research/projects/tao/download/tao-2.1-p2.tar.gz"
wget "http://www.unidata.ucar.edu/downloads/netcdf/ftp/netcdf-3.6.3.tar.gz"
tar -xzf netcdf-3.6.3.tar.gz
mkdir netcdf-3.6.3-install
cd netcdf-3.6.3
./configure --prefix=/home/$NAME/installz/netcdf-3.6.3-install 
make check > check.log
make install > install.log
cd ..
tar -xzf petsc-3.3-p7.tar.gz
cd petsc-3.3-p7
./configure PETSC_DIR=$PWD PETSC_ARCH=linux-gcc-cxx --download-f-blas-lapack=1 --download-mpich=1 --with-cc=gcc --with-cxx=g++ --with-fc=gfortran --with-clanguage=cxx --with-debugging=0
make PETSC_DIR=$PWD PETSC_ARCH=linux-gcc-cxx all
make PETSC_DIR=$PWD PETSC_ARCH=linux-gcc-cxx test
cd ..
tar -xzf tao-2.1-p2.tar.gz
cd tao-2.1-p2
make TAO_DIR=$PWD PETSC_DIR=/home/$NAME/installz/petsc-3.3-p7 PETSC_ARCH=linux-gcc-cxx all > make.log
make TAO_DIR=$PWD PETSC_DIR=/home/$NAME/installz/petsc-3.3-p7 PETSC_ARCH=linux-gcc-cxx tao_testfortran > fortran.log

export TACC_NETCDF_LIB='/home/$NAME/installz/netcdf-3.6.3-install/lib'
export TACC_NETCDF_INC='/home/$NAME/installz/netcdf-3.6.3-install/include'
export PETSC_DIR='/home/$NAME/installz/petsc-3.3-p7' 
#export PETSC_ARCH='linux-gcc-cxx-O3'
export PETSC_ARCH='linux-gcc-cxx'
#export PETSC_ARCH='linux-gcc-cxx-debug’
export TAO_DIR='/home/$NAME/installz/tao-2.1-p2'
export PATH=$PATH:/$PETSC_DIR/$PETSC_ARCH/bin
export PATH=$PATH:/home/$NAME/installz/netcdf-3.6.3-install/bin

#install RAPID
cd /home/$USER/work/
git clone https://github.com/c-h-david/rapid.git
cd rapid/src/
make rapid

#install HTCONDOR
apt-get install -y libvirt0 libdate-manip-perl vim
wget http://ciwckan.chpc.utah.edu/dataset/be272798-f2a7-4b27-9dc8-4a131f0bb3f0/resource/86aa16c9-0575-44f7-a143-a050cd72f4c8/download/condor8.2.8312769ubuntu14.04amd64.deb
dpkg -i condor8.2.8312769ubuntu14.04amd64.deb
#use this if master node and comment out following two lines
#echo CONDOR_HOST = \$\(IP_ADDRESS\)
echo CONDOR_HOST = 10.8.123.71 >> /etc/condor/condor_config.local
echo DAEMON_LIST = MASTER, SCHEDD, STARTD >> /etc/condor/condor_config.local
echo ALLOW_ADMINISTRATOR = \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_OWNER = \$\(FULL_HOSTNAME\), \$\(ALLOW_ADMINISTRATOR\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_READ = \$\(FULL_HOSTNAME\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_WRITE = \$\(FULL_HOSTNAME\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo START = True >> /etc/condor/condor_config.local
echo SUSPEND = False >> /etc/condor/condor_config.local
echo CONTINUE = True >> /etc/condor/condor_config.local
echo PREEMPT = False >> /etc/condor/condor_config.local
echo KILL = False >> /etc/condor/condor_config.local
echo WANT_SUSPEND = False >> /etc/condor/condor_config.local
echo WANT_VACATE = False >> /etc/condor/condor_config.local
. /etc/init.d/condor start
#NOTE: if you forgot to change lines for master node, change CONDOR_HOST = $(IP_ADDRESS)
# and run $ . /etc/init.d/condor restart as ROOT
