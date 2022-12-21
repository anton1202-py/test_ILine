import csv
from datetime import date
from datetime import datetime

import apiclient
import gspread
import httplib2
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


current_date = date.today()  # для поиска в логах по текущей дате

# чтение данных из файла логов и фильтрация данных по необходимым параметрам
with open('result_csv.log') as csvfile:
    new_data = []
    reader = csv.reader(csvfile)
    for row in reader:
        # Фильтруем данные log файла по дате и другим критериям
        if row[0] == str(current_date) and row[4] == '1044':
            new_data.append(row)

# Записываем отфильтрованные данные в новый файл - data.csv.
line_to_override = {}
with open('new_data.csv', 'w') as b:
    writer = csv.writer(b, delimiter=' ')
    writer.writerow(['Date/Time', 'Car ID', 'Shift', 'Type code',
                    'Color code', 'NoPaint', 'Fault', 'Ghost',
                    'ExtBodyID', 'R11.Paint consumption',
                    'R21.Paint consumption', 'TotalPaint'])
    for line, row in enumerate(new_data):
        data = line_to_override.get(line, row)
        writer.writerow(data)

# Преобразуем новый файл с данными в список
# Чтобы положить в одну ячейку в Google Sheets
df = pd.read_csv('new_data.csv')
answer = [''.join(df.values[:, i]) for i in range(len(df.columns))]


# файл с апи токенами
CREDENTIALS_FILE = 'test-progect-372221-c4b7c0e13614.json'
# id документа с которым будем работать
SPREADSHEET_ID = '1B9Tkv_geangyDMF8eAVZf_AiJdyLPaXQcUvO_hVmh8Q'


credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'])
# объект, который работает с аутентификацией
httpAuth = credentials.authorize(httplib2.Http())
# экземпляр апи с которым дальше будем работать
service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)


client = gspread.authorize(credentials)
# sheet = client.create("Test_google_sheet")
# sheet.share('kan198902@gmail.com', perm_type='user', role='writer')
sheet = client.open('Test_google_sheet').sheet1


# Дата начала первого запуска скрипта
start_date = datetime(day=20, month=12, year=2022)
now = datetime.now()
delta = now - start_date
# переменная необходима для динамического изменения номера ячейки
table_var = delta.days + 1
array_var = 'A'+str(table_var)
range_1 = array_var
# Загружаем преобразованные данные в ячейку
array = {'values': [answer]}
response = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID,
                                            range=range_1,
                                            valueInputOption='USER_ENTERED',
                                            body=array).execute()
