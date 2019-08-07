# Set up

## Set up [Git Large File Storage](https://git-lfs.github.com/)
1. Install git-lfs: https://github.com/git-lfs/git-lfs/wiki/Installation
2. Initialize Git LFS and its respective hooks in this repo:
```bash
git lfs install
```

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
On macOS:
```bash
python3 -m pip install -r requirements.txt
python3 -m pip install PyObjC
```
On Linux:
```bash
python3 -m pip install -r requirements.txt
```
On Windows:
```bash
pip install -r requirements.txt
```

## Download NLTK corpora
In the Python console, launch the NLTK Downloader:
```python
import nltk
nltk.download()
```
Download the following corpora:
- nps_chat

## Configure [Confuse](http://confuse.readthedocs.org) 
By default, Visual Studio Code will use [.env](.env) automatically 
when starting the Python environment, so add the following to the end of [.env](.env) (create the file if it does not exist):
```bash
# Confuse Config Search Directory
PULLREQUESTCOMMENTANALYZERDIR="./"
```
Alternatively, if not using Visual Studio Code, please see the instructions below.
### On macOS and Linux
1. Add the following to the end of venv [activate script](.venv/bin/activate):
```bash
# Confuse Config Search Directory
PULLREQUESTCOMMENTANALYZERDIR="./"
export PULLREQUESTCOMMENTANALYZERDIR
```
2. Then reactivate the venv to take effect:
```bash
source .venv/bin/activate
```
### On Windows
1. Add the following to the end of [Activate.ps1](.venv\Scripts\Activate.ps1):
```powershell
# Confuse Config Search Directory
$env:PULLREQUESTCOMMENTANALYZERDIR="./"
```
2. Then reactivate the venv to take effect:
```bash
.venv\Scripts\Activate.ps1
```