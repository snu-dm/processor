'''
RETURN TYPE
X organization sec
O name(ticker) tsla
O year 2021
X filing_type 10-K
X item_type 1
O documentdate

+ 추가로 파일명(html에서부터)
'''

import os
import numpy as np
import glob
import codecs
import pandas as pd
from bs4 import BeautifulSoup
import re, unidecode
from collections import OrderedDict
import json
from datetime import datetime

# Initial setup - currently set as a temporary file path in the current directory
current_path = os.path.dirname(__file__)
experiment_path = os.path.join(current_path, 'experiment_files')

df = pd.DataFrame(columns=[['ticker','text_length']])
file_paths = glob.glob('{}/*'.format(experiment_path), recursive=True)
print("Total number of files: " + str(len(file_paths)))

'''
def dic_concac(d):
    t = ''
    for key in d:
        t += key
        t += '\n'
        
        if len(d[key]) > 0:
            for key2 in d[key]:
                t += d[key][key2]
                t += '\n'

                if len[d[key][key2]] > 0:
                    for i in d[key][key2]:
                        t += d[key][key2][i]
                        t += '\n'

        t += '\n'
'''
for i in range(1): #range(len(file_paths)):
    
    print(str(i)+" processing started")

    infos = file_paths[i].split('\\')[-1].split('_')
    print(infos)
    print(file_paths[i])

    try:
        f = codecs.open(file_paths[i], 'r', 'utf-8')
        soup = BeautifulSoup(f.read(), 'lxml')
        text = unidecode.unidecode(soup.get_text('\n'))

        od = OrderedDict()

        item_1 = 'ITEM 1. BUSINESS'
        item_1a = 'ITEM 1A. RISK FACTORS'
        item_1b = 'ITEM 1B. UNRESOLVED STAFF COMMENTS'
        item_2 = 'ITEM 2. PROPERTIES'

        def is_end(text):
            return (item_1b in text) or (item_2 in text)
        
        def is_bt(text):
            return (item_1 in text) or (item_1a in text)

        st_tag = ''
        def is_st(text, name):
            global st_tag
            if st_tag == '' and ('Overview' in text) or ('OVERVIEW' in text):
                st_tag = name
                return True
            else:
                return name == st_tag and len(text) > 1
        
        mt_tag = ''
        prev = ('', '')
        def is_mt(text, name, prev):
            global mt_tag
            if mt_tag == '' and is_st(prev[0], prev[1]):
                mt_tag = name
                return True
            else:
                return name == mt_tag and len(text) > 1

        for tag in soup.find_all('span'):
            print(od)
            if tag.name and tag.attrs:
                tag_text = unidecode.unidecode(tag.get_text(strip=True))
                tag_name = " ".join([f'{key}="{value}"' for key, value in tag.attrs.items()])
                
                print(tag_text, tag_name)

                if is_end(tag_text):
                    break

                if is_bt(tag_text):
                    print('33')
                    if len(od) == 0:
                        od[item_1] = OrderedDict()
                    else:
                        od[item_1a] = OrderedDict()
                elif len(od) > 0 and is_st(tag_text, tag_name): # and len(od) > 0:
                    if len(od) == 1:
                        od[item_1][tag_text] = []
                    elif len(od) == 2:
                        od[item_1a][tag_text] = []
                    if prev == ('', ''):
                        prev = (tag_text, tag_name)
                elif len(od) > 0 and is_mt(tag_text, tag_name, prev): # and len(od) > 0:
                    od[next(reversed(od))][next(reversed(od[next(reversed(od))]))].append(tag_text)          

        print(st_tag, mt_tag)

        document = json.dumps(od, indent=8)
        
        # Saving the log - for experiments, need change when saving to database
        current_time = datetime.now().time()
        file_name = f"log_parser2_{current_time}.txt"

        with open(file_name, 'w') as log:
            log.write(document)

    except Exception as e:
        df.loc[len(df)] = [infos[0], 0]
        print("error : %s, Missed File URL: %s" % (e, file_paths[i]))
