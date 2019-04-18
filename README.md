# Set up
## Set up a [virtual environment](https://docs.python.org/3.6/library/venv.html#module-venv)
On macOS and Linux:
```bash
python3 -m venv .
source ./bin/activate
```
On Windows:
```bash
python -m venv .
.\Scripts\Activate.ps1
```

## Install dependent packages
On macOS and Linux:
```bash
python3 -m pip install -r requirements.txt
```
On Windows:
```bash
pip install -r requirements.txt
```