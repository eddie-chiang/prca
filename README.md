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
On macOS and Linux:
```bash
python3 -m pip install -r requirements.txt
```
On Windows:
```bash
pip install -r requirements.txt
```

## Download NLTK corpora
Open Python console:
```bash
.venv/bin/python3
```

In the Python console, launch the NLTK Downloader:
```python
import nltk
nltk.download()
```
Download the following corpora:
- nps_chat