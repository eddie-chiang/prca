Pull Request Comments Aggregator (PRCA) is an aggregator service to load GitHub Pull Request Comments data from the following sources:
1. [GHTorrent's MongoDB](https://ghtorrent.org/raw.html)
2. [GitHub REST API v3](https://developer.github.com/v3/)
# Set up

## Set up a [virtual environment](https://docs.python.org/3.6/library/venv.html#module-venv)
On macOS and Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
On Windows:
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
```

## Install dependent packages
```bash
pip install -r requirements.txt
```

## Configure [Confuse](http://confuse.readthedocs.org) 
By default, Visual Studio Code will use [.env](.env) automatically 
when starting the Python environment, so add the following to the end of [.env](.env) (create the file if it does not exist):
```bash
# Confuse Config Search Directory
PRCADIR="./"
```
Alternatively, if not using Visual Studio Code, please see the instructions below.
### On macOS and Linux
1. Add the following to the end of venv [activate script](.venv/bin/activate):
```bash
# Confuse Config Search Directory
PRCADIR="./"
export PRCADIR
```
2. Then reactivate the venv to take effect:
```bash
source .venv/bin/activate
```
### On Windows
1. Add the following to the end of [Activate.ps1](.venv\Scripts\Activate.ps1):
```powershell
# Confuse Config Search Directory
$env:PRCADIR="./"
```
2. Then reactivate the venv to take effect:
```bash
.venv\Scripts\Activate.ps1
```