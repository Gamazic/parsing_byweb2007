import os
import bz2
import base64
import re
import pickle

from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd


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
    except:
        continue

# Pulling out id and text of html pages from .xml
iterator = list(range(36))
for i in iterator:
    docs_list = []
    print('iter', i)
    name_of_file = f'byweb{i}.xml'
    # Some archives are corrupted
    try:
        tree = ET.parse(name_of_file)
        sroot = tree.getroot()
        wdict = etree_to_dict(sroot)
    except:
        continue
    docs_num = len(wdict['dataset']['document'])
    for j in tqdm(range(docs_num)):
        raw_data = wdict['dataset']['document'][j]['content']['#text']
        encoded = raw_data.encode('ascii')
        html_data = base64.b64decode(encoded)
        text = BeautifulSoup(html_data).get_text()
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

# After pulling out and saving the information from .xml we can load all files and save it in full
files = [filename for filename in os.listdir('pickled_docs') if filename.startswith('doc')]

docs = []
for f_name in files:
    with open('pickled_docs/' + f_name, 'rb') as f:
        docs.extend(pickle.load(f))

# As you prefer you can save the files. 
# For example:
pd.DataFrame(docs, columns=['id', 'text']).to_csv('byweb2007.csv', index=False)