import os
import bz2
import base64
import re
import pickle
from collections import defaultdict
from lxml import etree
import xml.etree.ElementTree as ET
import traceback

from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd

#tree = ET.parse('simple.xml')
#sroot = tree.getroot()

def hasAttribs(t):
    """ проверяет наличие атрибутов в узле и
    исключает вырожденные атрибуты типа {http://www.w3.org/2001/XMLSchema-instance}nil """

    if not t.attrib:
        return False

    for a in t.attrib:
        if a.find('{http://www.w3.org/2001/XMLSchema-instance}nil')>=0:
            return False

    return True

def etree_to_dict(t):
    """ конвертилка распарсенного XML в словарь """
    
    # удалим наймспейс из тэга
    if t.tag.find('{')>=0:
        t.tag = t.tag.split('}')[1]

    # учтем здесь {http://www.w3.org/2001/XMLSchema-instance}nil
    d = {t.tag: {} if hasAttribs(t) else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if hasAttribs(t):
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

def getByDot(d,ss):
    """ выбирает из словаря d элемент по строке ss в точечной нотации """

    for e in ss.split("."):
        if not isinstance(d, (dict,)):
            return None
        if e in d.keys():
            d = d[e]
        else:
            return None

    return d

def xml_to_dict(name_of_file):
    '''Парсинг xml в словарь'''

    parser = etree.XMLParser(recover=True)
    tree = ET.parse(name_of_file, parser=parser)
    sroot = tree.getroot()
    wdict = etree_to_dict(sroot)
    return wdict

def fix_xml(file_path, encode_docurl=False):
    '''
    Либо некоторые xml собирал чел после 12-часовой смены, либо им как то за 14 лет стало плохо:
    -3, 4: строка по тегу <docURL> не закодирована в base64 (проблема в специфичных для xml символов типа "&"), поэтому кодируется тут
    -3, 4, 5: нет <docURL>, <docID> у последнего <document> и </romip:dataset> в целом, поэтому последний <document> удален и </romip:dataset> добавлен
    -17: archive corrupted - invalid data stream
    -25: docID 1069923 - HTML parsing error 
    -кривой xml удаляется, и создается новый с таким же именем
    '''
    with open(file_path) as input_file, open('temp.xml', 'w') as output_file:
        contents = input_file.readlines()
        if encode_docurl:
            for line in contents[:-1]:
                if line.rfind('<docURL>') > -1:    
                    start = line.rfind('<docURL>') + len('<docURL>')
                    end = line.find('</docURL>')
                    message = line[start:end]
                    message_bytes = message.encode('ascii')
                    base64_bytes = base64.b64encode(message_bytes)
                    base64_message = base64_bytes.decode('ascii')
                    new_line = '<docURL>' + base64_message + '</docURL>'
                else:
                    new_line = line
                output_file.write(new_line)
            output_file.write('</romip:dataset>')
        else:
            contents[-1] = '</romip:dataset>'
            output_file.writelines(contents)
    os.remove(file_path)
    os.rename('temp.xml', file_path)

def make_task_csv(input_file, output_file):
    '''
    Создание таблицы запросов из xml файла и сохранение в csv
    '''

    wdict = xml_to_dict(input_file)

    tasks = wdict['task-set']['task']
    tasks_list = []
    for j in range(len(tasks)):
        tasks_list.append((tasks[j]['@id'], tasks[j]['querytext']))
    tasks_df = pd.DataFrame(tasks_list, columns=['task id', 'query text'])
    tasks_df.to_csv(output_file, index=False)

def make_relevance_csv(input_file, output_file):
    '''
    Создание таблицы релевантности и сохранение в csv
     '''

    wdict = xml_to_dict(input_file)

    tasks = wdict['taskDocumentMatrix']['task']
    tasks_list = []
    doc_dict = {}
    for task in tasks:
        task_id = task['@id']
        tasks_list.append(task_id)
        for doc in task['document']:
            doc_id = doc['@id']
            if doc_id not in doc_dict:
                doc_dict[doc_id] = {}
            doc_dict[doc_id][task_id] = doc['@relevance']
    sorted_doc_dict = sorted(doc_dict.items(), key=lambda x: x[0])
    
    relevance_dict = {}
    docs_list = [i[0] for i in sorted_doc_dict]
    relevance_dict['docID'] = docs_list
    for doc in docs_list:
        for task in tasks_list:
            if task in doc_dict[doc]:
                if task not in relevance_dict:
                    relevance_dict[task] = [doc_dict[doc][task]]
                else:
                    relevance_dict[task].append(doc_dict[doc][task])
            else:
                if task not in relevance_dict:
                    relevance_dict[task] = ['None']
                else:
                    relevance_dict[task].append('None')
    relevance_df = pd.DataFrame(relevance_dict)
    relevance_df.to_csv(output_file, index=False)
    
if __name__ == '__main__':

    # Ids of .xml files with various problems
    problem_files_ids = [3, 4, 5, 17, 25]

    # Pulling out .xml files from bz2 archives
    for i in range(36): 
        path = f'byweb2007/byweb.xml.{i}.bz2'
        name_of_file = f'byweb{i}.xml'
        # Some archives are corrupted for some reason
        try:
            with open(name_of_file, 'wb') as new_file, open(path, 'rb') as file:
                decompressor = bz2.BZ2Decompressor()
                for data in iter(lambda : file.read(100 * 1024), b''):
                    new_file.write(decompressor.decompress(data))
        except Exception:
            print(f'!!! Could not unzip {path}: ')
            traceback.print_exc()
        # Fixing problematic .xml files
        try:
            if i in problem_files_ids:
                if i in [3, 4]:
                    fix_xml(name_of_file, encode_docurl=True)
                elif i in [5, 17]:
                    fix_xml(name_of_file, encode_docurl=False)
        except Exception:
            print(f'!!! Could not fix {name_of_file}: ')
            traceback.print_exc()
            continue

    # Pulling out id and text of html pages from .xml
    iterator = list(range(36)) 
    for i in iterator:
        docs_list = []
        print('iter', i)
        name_of_file = f'byweb{i}.xml'
        # Some archives are corrupted
        try:
            wdict = xml_to_dict(name_of_file)
        except Exception:
            print(f'XML PARSING ERROR {name_of_file}: ')
            traceback.print_exc()
            continue
        docs_num = len(wdict['dataset']['document'])
        for j in tqdm(range(docs_num)):
            raw_data = wdict['dataset']['document'][j]['content']['#text']
            encoded = raw_data.encode('ascii')
            html_data = base64.b64decode(encoded)
            try:
                text = BeautifulSoup(html_data, features="html.parser").get_text()
            except Exception:
                print('HTML PARSING ERROR!: ')
                traceback.print_exc()
                continue
            # Simple preprocessing for my task purposes
            text = text[:1000]
            text = re.sub("^\s+|\n|\t|\r|\s+$", ' ', text)
            text = re.sub('\xa0', '', text)
            text = re.sub(r'[^а-яА-Яa-zA-Z0-9,.!?:;\- ]', ' ', text)
            text = re.sub(r'((?<=^)|(?<= )).((?=$)|(?= ))', '', text)
            text = re.sub(r'\s+', ' ', text)
            docs_list.append(
                                (int(wdict['dataset']['document'][j]['docID']), text)
            )
        # I do this as a protection against bugs
        with open(f'pickled_docs/doc{i}.pickle', 'wb') as f:
            pickle.dump(docs_list, f)
        # Deleting .xml file 
        os.remove(name_of_file)

    # After pulling out and saving the information from .xml we can load all files and save it in full
    files = [filename for filename in os.listdir('pickled_docs') if filename.startswith('doc')]

    docs = []
    for f_name in files:
        with open('pickled_docs/' + f_name, 'rb') as f:
            docs.extend(pickle.load(f))

    # As you prefer you can save the files. 
    # For example:
    pd.DataFrame(docs, columns=['id', 'text']).to_csv('byweb2007.csv', index=False)

    # making and saving task csv from http://romip.ru/tasks/2008/web2008_adhoc.xml.bz2 
    make_task_csv('web2008_adhoc.xml', 'tasks_2008.csv')
    # making and saving relevance csv from http://romip.ru/relevance-tables/ru/index.html 
    make_relevance_csv("2009_or_relevant-minus_table.xml", 'relevance_OR_2009.csv')


