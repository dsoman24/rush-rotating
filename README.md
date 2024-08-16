# rush-rotating

## Setup
1. Create a Python virtual environment
```shell
python3 -m venv .venv
```

2. Install dependencies
```shell
pip install -r requirements.txt
```

3. Create the environment file
```shell
touch src/.env
```
Required environment variables:
- `ATLAS_URI`: MongoDB Atlas URI to the RushKit project
(e.g. `ATLAS_URI=mongodb+srv://...`)
- `DB_NAME`: MongoDB database name
- `SHEET_ID`: Google Sheets API spreadsheet ID

4. Add your service account key

First, create the keys directory
```shell
mkdir src/keys
```
Add the Google Cloud APIs service account private key json to this directory.

## Executing the rotator script

The driver for the rotator script is in `src/main.py`. To execute, run
```shell
python3 src/main.py
```
Logs are found in `main.log`.

## Additional Information
* Google Cloud project link:
https://console.cloud.google.com/welcome?project=rush-428023

## TODO
- [] Write rows to sheet
- [] Benchmark runtime to establish a good period for continuous runs