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

def dic_concac(d):
    t = ''
    for key in d:
        t += str(key)
        t += '\n'
        
        if len(d[key]) > 0:
            for key2 in d[key]:
                t += str(key2)
                t += '\n'

                if len(d[key][key2]) > 0:
                    for i in range(len(d[key][key2])):
                        t += d[key][key2][i]
                        t += '\n'
                t += '\n'

        t += '\n'
    return(t)

item_1 = 'ITEM 1. BUSINESS'
item_1a = 'ITEM 1A. RISK FACTORS'
item_1b = 'ITEM 1B. UNRESOLVED STAFF COMMENTS'
item_2 = 'ITEM 2. PROPERTIES'

st_tag = ''
mt_tag = ''
prev = ('', '')

def is_st(text, name):
    global st_tag
    global prev
    if st_tag == '' and ('Overview' in text) or ('OVERVIEW' in text):
        st_tag = name
        return True
    else:
        return name == st_tag and len(text) > 1

def is_mt(text, name, prev):
    global mt_tag
    if mt_tag == '' and is_st(prev[0], prev[1]):
        mt_tag = name
        return True
    else:
        return name == mt_tag and len(text) > 1

def parse_single(file_txt):
    df = pd.DataFrame(columns=[['ticker','text_length']])
    st_tag = ''
    mt_tag = ''
    prev = ('', '')

    try:
        soup = BeautifulSoup(file_txt.read(), 'lxml')
        text = unidecode.unidecode(soup.get_text('\n'))

        od = OrderedDict()

        
        date_pattern = re.compile(r'\bFILED AS OF DATE:\s*(\d{8})\b', re.IGNORECASE)
        filename_pattern = re.compile(r'\S+\.htm')
        date_val = int(date_pattern.findall(text)[0])
        filename_val = filename_pattern.search(text)[0][:-4]
        print(date_val, filename_val)

        for tag in soup.find_all('span'):
            
            if tag.name and tag.attrs:
                tag_text = unidecode.unidecode(tag.get_text(strip=True))
                tag_name = " ".join([f'{key}="{value}"' for key, value in tag.attrs.items()])
                
                #print(tag_text, tag_name)

                if (item_1b in tag_text) or (item_2 in tag_text):
                    break

                if (item_1 in tag_text) or (item_1a in tag_text):
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

        #document = json.dumps(od, indent=8)
        
        # Saving the log - for experiments, need change when saving to database
        current_time = datetime.now().time()
        
        file_name = f"log_parser2_0205_{current_time}.txt"
        with open(file_name, 'w') as log:
            log.write(dic_concac(od))

        r_doc = dic_concac(od)

        return(r_doc, filename_val, date_val) # parsed data, file name(ex: abbv-20191231), file date(ex: 20200221)

    except Exception as e:
        print("error : %s" % (e))
