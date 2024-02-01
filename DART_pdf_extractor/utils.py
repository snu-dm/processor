import os
import re
import pickle
from math import dist
import pdfplumber


# folder_path의 모든 파일 이름을 확장자 포함하여 리스트로 추출
def get_file_names(folder_path):
    if not os.path.exists(folder_path):
        folder_name = folder_path.split('/')[-1]
        exit(f'FolderNotExistError: There is no [{folder_name}] folder')
    
    # filter only pdf files
    pdf_file_lists = [filename for filename in os.listdir(folder_path) if filename.lower().endswith('.pdf')]

    if not pdf_file_lists:
        exit('FileNotExistError: There are no files in the folder')

    return sorted(pdf_file_lists)

# folder_path의 모든 파일 path를 리스트로 추출
def get_file_paths(folder_path):

    ## list file name
    pdf_file_lists = get_file_names(folder_path=folder_path)

    pdf_file_paths = [folder_path + '/' + file_name for file_name in pdf_file_lists]

    return sorted(pdf_file_paths)

# path로부터 파일 이름 구하기(path 맨 마지막 문자열 추출)
def path_to_name(path):
    name = path.split('/')[-1]
    return name

# bbox 'list'의 모든 bbox에 padding을 적용하여 리스트로 추출
def bbox_padding(bbox_list, padding=0):
    new_bbox = []
    for bbox in bbox_list:
        new_bbox.append((bbox[0], max(bbox[1]-padding, 0), bbox[2], min(bbox[3]+padding, 842)))
    return new_bbox

# 파일 path의 파일을 pdfplumber의 pdf 형식으로 반환
def get_pdf(path):
    pdf = pdfplumber.open(path)
    return pdf

# pdf의 모든 page를 pdfplumber의 page 형식으로, 리스트로 반환
def get_pages(pdf):
    pages = pdf.pages
    return pages

# pdfplumber의 page 객체 내 text 객체를 추출
def get_text(page):
    text = page.extract_words()
    return text

# pdfplumber의 page 객체 내 tableobject 객체를 리스트로 추출
def get_table(page):
    table = page.find_tables()
    return table

# pdfplumber의 page 객체 내 image 객체를 추출
def get_image(page):
    image = page.images
    return image

# pdfplumber의 tableobject 객체를 dict형식으로 바꾸어 추출(cells, bbox, extract)
def table_object_to_dict(table_object):
    table_dict = {}
    table_dict['cells'] = table_object.cells
    table_dict['bbox'] = table_object.bbox
    table_dict['extract'] = table_object.extract()
    return table_dict

# 저장 path와 파일이름을 받고 해당 save_path에 file_name 명의 디렉토리 생성
def make_pdf_dir(save_path, file_name):
    pdf_save_directory = save_path + '/' + file_name[:-4]

    ## make directory for file
    if not os.path.exists(pdf_save_directory):
        os.makedirs(pdf_save_directory)

# data와 저장경로, 해당 data가 유래한 pdf의 파일면, data_type(text, image, table)을 인수로 받아 저장
def save_pickle_file(data, save_path, file_name, data_type):
    with open(f'{save_path}/{file_name[:-4]}/{file_name[:-4]}_{data_type}.pickle', 'wb') as fw:
        pickle.dump(data, fw)

# 2개의 직사각형 사이의 최단거리 계산
# rotation은 고려 x
# https://stackoverflow.com/questions/4978323/how-to-calculate-distance-between-two-rectangles-context-a-game-in-lua
def rect_distance(RectA, RectB):
    A_x0, A_y0, A_x1, A_y1 = RectA
    B_x0, B_y0, B_x1, B_y1 = RectB
    
    # 변수명: A 기준
    left = B_x1 < A_x0 
    right = A_x1 < B_x0
    top = A_y1 < B_y0
    bottom = B_y1 < A_y0
    
    if top and left:
        return dist((A_x0, A_y1), (B_x1, B_y0))
    elif left and bottom:
        return dist((A_x0, A_y0), (B_x1, B_y1))
    elif bottom and right:
        return dist((A_x1, A_y0), (B_x0, B_y1))
    elif right and top:
        return dist((A_x1, A_y1), (B_x0, B_y0))
    elif left:
        return A_x0 - B_x1
    elif right:
        return B_x0 - A_x1
    elif bottom:
        return A_y0 - B_y1
    elif top:
        return B_y0 - A_y1
    else: # rectangles intersect
        return 0.0

# 2개의 직사각형 상하관계만 판단
def top_or_bottom(RectA, RectB):
    _, A_y0, _, A_y1 = RectA
    _, B_y0, _, B_y1 = RectB
    
    # 변수명: A 기준
    top = A_y1 < B_y0
    bottom = B_y1 < A_y0

    # A가 위에 있음
    if top == True and bottom == False:
        return 1
    # B가 위에 있음
    elif top == False and bottom == True:
        return -1
    # 2개의 직사각형이 intersect
    else:
        return 0

# y값의 차이만 계산 (수직 거리)
# rect_distance에서 A와 B의 x0과 x1은 같은 경우
def diff_height(RectA, RectB):
    _, A_y0, _, A_y1 = RectA
    _, B_y0, _, B_y1 = RectB

    # 변수명: A 기준
    top = A_y1 < B_y0
    bottom = B_y1 < A_y0

    # A가 위에 있음
    if top == True and bottom == False:
        return B_y0 - A_y1
    # B가 위에 있음
    elif top == False and bottom == True:
        return A_y0 - B_y1
    # 2개의 직사각형이 intersect
    else:
        return 0.0

# RectA가 RectB를 포함하는지 체크
# https://stackoverflow.com/questions/21275714/check-rectangle-inside-rectangle-in-python
def contains(RectA, RectB):
    A_x0, A_y0, A_x1, A_y1 = RectA
    B_x0, B_y0, B_x1, B_y1 = RectB

    return A_x0 < B_x0 < B_x1 < A_x1 and A_y0 < B_y0 < B_y1 < A_y1

# image, text에서 사용
def get_bbox(obj):
    bbox = (round(obj['x0'], 2), round(obj['top'], 2), round(obj['x1'], 2), round(obj['bottom'], 2))
    return bbox


