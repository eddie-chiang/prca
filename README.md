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
In the Python console, launch the NLTK Downloader:
```python
import nltk
nltk.download()
```
Download the following corpora:
- nps_chat

## Configure Google BigQuery Service Account
To use the google.cloud.bigquery library, it expects a environment variable for retrieving the service account credential JSON file ([instructions](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries)).

On Windows, add the following to [Activate.ps1](.venv\Scripts\Activate.ps1):
```PowerShell
# Google BigQuery
$env:GOOGLE_APPLICATION_CREDENTIALS="$env:VIRTUAL_ENV\..\bigquery\BigQueryServiceAccount.json"
```