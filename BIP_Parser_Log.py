import re  # Библиотека для использования регулярных выражений.
from elasticsearch import Elasticsearch  # Библиотека для взаимодействия с ES.
import datetime
 
 
# Функция чтения файла и подготовки данных.
def read_file(file_path):
    # Открываем файл с логом и записываем его содержимое в переменную f
    with open(file_path, 'r') as f:
        # Записываем в переменную regexp_datetime регулярное выражение (маска), которая поможет найти дату и время операции в логе.
        regexp_datetime = r'\w{3}\ \d{1,2}\,\ \d{4}\ \d{1,2}\:\d{1,2}\:\d{1,2}\,\d{1,4}\ \w{2}\ \w{3}'
        # Регулярное выражение для поиска print_form
        regexp_print_form = r'reports/\SiebelCRMReports\%\w*|\/MergePDFServlet'
        # Регулярное выражение для поиска ExecuteThread
        regexp_ExecuteThread = r'ExecuteThread\:\ \'\d{1,10}'
        # Регулярное выражение для поиска seconds_busy
        regexp_seconds_busy = r'has been busy for \"\d{1,10}'
        # Создаем пустой список, чтобы потом им воспользоваться.
        data = []
        # Делаем цикл по файлу, где каждая строчка записывается в переменную row.
        # Запоминаем в переменную фразу, которую нужно найти в строке.
        search_word = '<BEA-000337> <[STUCK] ExecuteThread'
        for row in f:
            # Проверяем, встречается ли фраза в строке.
            error_word = row.find(search_word)
            # Если фраза встречается, то выплняется условие.
            if error_word != -1:
                # Ищем в строчке дату и время по ранее сформированную регулярному выражению и записываем её в переменную.
                date_time_buffer = re.findall(regexp_datetime, row)
                date_time_buffer = datetime.datetime.strptime(date_time_buffer[0], '%b %d, %Y %I:%M:%S,%f %p MSK')
                date_time = date_time_buffer.strftime("%Y-%m-%dT%H:%M:%S+03:00")
                # Запоминаем строку в переменную error_text, для последующего вывода в сообщение.
                error_text = row
                # Ищем в строчке print_form по ранее сформированную регулярному выражению и записываем её в переменную.
                print_form = re.findall(regexp_print_form, row)
                # Ищем в строчке execute_thread по ранее сформированную регулярному выражению и записываем её в переменную.
                execute_thread = re.findall(regexp_ExecuteThread, row)
                # Ищем в строчке seconds_busy по ранее сформированную регулярному выражению и записываем её в переменную.
                seconds_busy = re.findall(regexp_seconds_busy, row)
                stuck_id = str(date_time_buffer.strftime("%d%m%Y%H%M%S%f")) + str(
                    execute_thread[0].replace('ExecuteThread', '').replace(':', '').replace(' ', '').replace("'", ''))
                stuck_index = 'crm-bip-log-' + str(date_time_buffer.strftime("%Y.%m"))
                # Добавляем в список результат итерации цикла, в которой найден STUCK. Удаляем лишнии символы и фразы (replace).
                data.append(dict([('Date_time', date_time), ('Print_form', print_form[0].replace('/', '')),
                                  ('Execute_thread',
                                   execute_thread[0].replace('ExecuteThread', '').replace(':', '').replace(' ',
                                                                                                           '').replace(
                                       "'", '')),
                                  ('Seconds_busy', seconds_busy[0].replace('has been busy for ', '').replace('"', '')),
                                  ('Error_text', error_text), ('Stuck_id', stuck_id), ('Stuck_index', stuck_index)]))
    # Возвращаем результат функции.
    return data
 
# Функция отправки данных в ES.
def insert_data_to_es(data):
    try:
        # Создаем подключение к ES
        elastic = Elasticsearch(
            ['rbccpis00001.gts.rus.socgen'], port=9200)
    except Exception as err:
        print('Error create connection.', err)
    else:
        print('Connect OK.')
        # Передаем JSON в ES
        try:
            for i in data:
                elastic_id = i['Stuck_id']
                elastic_index = i['Stuck_index']
                if elastic.exists(index=elastic_index, id=elastic_id):
                    continue
                else:
                    if elastic.indices.exists(index=elastic_index):
                        elastic.index(index=elastic_index, doc_type="bipstucklog", id=elastic_id, body=i)
                        print('Row loaded successfully', elastic_index, elastic_id)
                    else:
                        elastic.indices.create(index=elastic_index, ignore=400)
                        mapping = {
                            "properties": {"Date_time": {"type": "date"},
                                           "Print_form": {"type": "text"},
                                           "Execute_thread": {"type": "integer"},
                                           "Seconds_busy": {"type": "integer"},
                                           "Error_text": {"type": "text"},
                                           "Stuck_id": {"type": "text"},
                                           "Stuck_index": {"type": "text"}}}
                        elastic.indices.put_mapping(index=elastic_index, doc_type='bipstucklog', body=mapping,
                                                    include_type_name=True)
                        elastic.index(index=elastic_index, doc_type="bipstucklog", id=elastic_id, body=i)
                        print('Row loaded successfully:', elastic_index, elastic_id)
        except Exception as err:
            print('Error insert data.', err)
        else:
            print('Insert completed.')
 
# Вызов функции (по сути это старт скрипта).
if __name__ == '__main__':
    # Запускаем одну функцию в другой. Передаем на вход директорию и название файла.
    # Если файл py лежит в той же директории, то можно оставить такой же путь, если в другой, то придется дописать.
    insert_data_to_es(read_file('/opt/BIP/Middleware/Oracle_Home/user_projects/domains/bi/servers/bi_server2/logs/bi_server2.out'))