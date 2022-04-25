# snakemake_ssh
**Snakemake for multiple nodes by using SSH connections**  
Snakemake in Slurm/LSF/BSUB cluster is officially supported.  
By using this script, you can also run Snakemake in SSH-cluster.  

## Install
  git clone https://github.com/StarSkyZheng/snakemake_ssh.git ~/.config/snakemake/ssh
  
## Usage
### 1. Run server
  cd ~/.config/snakemake/ssh  
  ./server.py -c server.py.configs.yaml.default  
### 2. Run snakemake
  cd WORKDIR_WITH_Snakefile  
  snakemake  --profile ssh ......parameters.......  
  **Note: when you force kill the server, jobs running in each nodes may not stop, which means you have to manually check and killed for each node.**  
  
## Config server and nodes
  Nodes and CPUs were defined in server.py.configs.yaml.default  
  Nodes and CPUs can be changed even when server is running. Deleted node will not get new jobs, but the existed running job will not be killed.  
  Default port is 23456, which is changeable. Must be same between config.yaml, ssh-status.py and ssh_utils.py  

## Check jobs status
  STDOUT, STDERR and retcode can be found in `server.py.configs.yaml.default.pickle` (can be changed in config.yaml)  
  Here are some script you can use: `view_jobs.sh` and `view_jobs.py`.   
  Try `view_jobs.sh` first. This script based on ipython, which requires users know basic usage of python.  
  Never try to modifiy `server.py.configs.yaml.default.pickle` when server is running. Treated as read-only file.  
  
