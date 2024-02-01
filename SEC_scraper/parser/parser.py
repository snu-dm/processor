import os
import pandas as pd
import numpy as np
import glob
import codecs
from bs4 import BeautifulSoup
import re, unidecode
from gensim.models.doc2vec import TaggedDocument
from gensim.models import Doc2Vec
import nltk
from scipy.cluster.hierarchy import dendrogram, linkage
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.font_manager
#matplotlib.font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
#matplotlib.font_manager.fontManager.ttflist
matplotlib.rc('font', family='Malgun Gothic') 

# Initial setup
# Currently set as a temporary file path in the current directory
current_path = os.path.dirname(__file__)
experiment_path = os.path.join(current_path, "experiment_files")

df = pd.DataFrame(columns=[['ticker','text_length']])
p_item1_start = re.compile('(item)\s+[1I]\s*[.:-]*\s*\w{0,5}\s*(business)', re.I)
p_item2_start = re.compile('(item)\s+[2I]+\s*[.:-]*\s*\w*\s*\w*\s*(properties|communitiesn)', re.I)
table_pattern = re.compile('\s+[0-9]+\s+(table of contents)\s+', re.I) # table of contents 제거

file_paths = glob.glob('{}/*'.format(experiment_path), recursive=True)
print("Total number of files: " + str(len(file_paths)))

for i in range(1): #range(len(file_paths)):
    print(str(i)+" processing started")

    infos = file_paths[i].split('\\')[-1].split('_')

    try:
        f = codecs.open(file_paths[i], 'r', 'utf-8')
        soup = BeautifulSoup(f.read(), 'lxml')
        text = unidecode.unidecode(soup.get_text('\n'))

        start = p_item1_start.finditer(text)
        lst = []
        for s in start:
            lst.append(s)
        if len(lst) < 2:
            start_idx = lst[-1].span()[1]
        else:
            start_idx = lst[1].span()[1]

        end = p_item2_start.finditer(text)
        lst = []
        for e in end:
            lst.append(e)
        if len(lst) < 2:
            end_idx = lst[-1].span()[0]
        else:
            end_idx = lst[1].span()[0]

        document = table_pattern.sub('\n\n\n', text[start_idx:end_idx])

    except Exception as e:
        df.loc[len(df)] = [infos[0], 0]
        print("error : %s, Missed File : %s, Missed File URL: %s" % (e, infos[:1], file_paths[i]))

    # Saving the log - for experiments, need change when saving to database

    file_name = f"log3_{i}.txt"

    with open(file_name, 'w') as log:
        log.write(document)
