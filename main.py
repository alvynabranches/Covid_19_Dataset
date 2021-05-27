import os, json, requests, lxml.html as lh, numpy as np, pandas as pd, socket, sys, subprocess as sp
from datetime import datetime as dt
from time import perf_counter, sleep
from flask import Flask, send_file
from threading import Thread

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
DEBUG = False

app = Flask('')

@app.route('/')
def home():
    file = os.path.join(DATA_DIR, 'worldometer_covid_dataset.csv')
    if os.path.isfile(file):
        rows, bytes_size = pd.read_csv(file).shape[0], os.stat(file).st_size / 1024 / 1024
        return '<b>Hello, I am alive!!!</b><br>CSV File has %s rows.<br> The size of the CSV file is %f MB' % (rows, bytes_size)
    else:
        return '<b>Hello, I am alive!!!</b><br>No CSV File Found.'

@app.route('/delete', methods=['POST'])
def delete():
    file = os.path.join(DATA_DIR, 'worldometer_covid_dataset.csv')
    if os.path.isfile(file):
        return json.dumps({'File_Status': 'File found'})
    else: return json.dumps({'File_Status': 'File not found'})
    
@app.route('/download', methods=['GET'])
def download():
    file = os.path.join(DATA_DIR, 'worldometer_covid_dataset.csv')
    return send_file(file, as_attachment=True) if os.path.isfile(file) else json.dumps({'File_Status': 'File not found'})

def run_server():
    app.run(host='0.0.0.0', port=8000)
  
def keep_alive():
    t = Thread(target=run_server)
    t.start()


def check_if_file_exists(file, ff):
    for i in range(1, 2000):
        new_file = f'{file}_{i}'
        if not os.path.isfile(f'{new_file}{ff}'): return new_file

def scrape(file, format='xlsx', debug=True):
    if not (format == 'xlsx' or format == 'xls' or format == 'csv'): raise ValueError('Invalid format')
    date_time = str(dt.now())
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    TRS = lh.fromstring(requests.post('https://www.worldometers.info/coronavirus/').content).xpath('//tr')
    if format == 'xlsx' or format == 'xls': df = pd.read_excel(f'{file}.{format}')  if os.path.isfile(f'{file}.{format}') else None
    else: df = pd.read_csv(f'{file}.{format}') if os.path.isfile(f'{file}.{format}') else None
    cols, first = ['datetime'], True
    for row in TRS:
        i, row_data = 0, dict()
        for col in row:
            if first: cols.append(str(col.text_content()).replace('\n', ''))
            else: 
                if sys.version.startswith('3.9'):
                    row_data |= {cols[i+1]:str(col.text_content()).replace('\n', '')}
                else: row_data = {**row_data, **{cols[i+1]:str(col.text_content()).replace('\n', '')}}
            i += 1
        if first: 
            first = False
        else:
            if sys.version.startswith('3.9'):
                row_data |= dict(datetime=date_time)
            else: row_data = {**row_data, **dict(datetime=date_time)}
        df = pd.DataFrame(columns=cols) if type(df) != pd.DataFrame else df.append(row_data, ignore_index=True)
        del col
    df = df.drop_duplicates()
    if debug: print(df.shape[0])
    df = df[~(df['Country,Other']=='Total:')]
    if debug: print(df.shape[0])
    df = df[~(df['Country,Other']==np.nan)]
    if debug: print(df.shape[0])
    df = df[~(df['Country,Other']=='')]
    if debug: print(df.shape[0])
    df = df[~(df['Country,Other']=='Country,Other')]
    if debug: print(df.shape[0])
    df = df[~(df['#']=='')]
    if format == 'xlsx' or format == 'xls':
        df.to_excel(f'{file}.{format}', index=False)
    else:
        df.to_csv(f'{file}.{format}', index=False)
    del cols, TRS, row, debug, date_time
    return file, df.shape[0]

def git_push(rows, debug=True):
    if not debug:
        try: sp.call('git config --global user.email "alvynabranches@gmail.com"')
        except Exception as e: print(e)
        try: sp.call('git config --global user.name "alvynabranches"')
        except Exception as e: print(e)
        sp.call(f'git pull https://alvynabranches:{os.environ["GITHUB_PASSWORD"]}@github.com/alvynabranches/Covid_19_Dataset.git')
        sp.call('git add .')
        sp.call(f'git commit -m {str(dt.now())}_rows={rows}')
        sp.call(f'git push https://alvynabranches:{os.environ["GITHUB_PASSWORD"]}@github.com/alvynabranches/Covid_19_Dataset.git --all')

def run():
    i, file = 0, os.path.join(DATA_DIR, 'worldometer_covid_dataset')
    try:
        while True:
            if socket.gethostbyname(socket.gethostname()) != '127.0.0.1':
                s = perf_counter()
                file, rows = scrape(file, 'csv', debug=DEBUG)
                # git_push(rows, debug=DEBUG)
                e = perf_counter()
                t = e - s
                print(f'Time Taken -> {round(t, 2)} seconds'); sleep(abs(600 - t))
            else: sleep(1); print('.'*i, end='\r')
            i += 1
    except KeyboardInterrupt: pass
    finally: del i

if not DEBUG: keep_alive()
run()