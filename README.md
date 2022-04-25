# snakemake_ssh
Snakemake for multiple nodes by SSH connection

## Install
  git clone https://github.com/StarSkyZheng/snakemake_ssh.git ~/.config/snakemake/ssh
  
## Usage
### 1. Run server
  cd ~/.config/snakemake/ssh  
  ./server.py -c server.py.configs.yaml.default  
### 2. Run snakemake
  cd WORKDIR_WITH_Snakefile  
  snakemake  --profile ssh ......parameters.......  
  
## Config server and nodes
  Nodes and CPUs were defined in server.py.configs.yaml.default  
  Nodes and CPUs can be changed even when server is running. Deleted node will not get new jobs, but the existing job will not be killed.  
  Default port is 23456, which is changeable. Must be same between config.yaml, ssh-status.py and ssh_utils.py  
  
