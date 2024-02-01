import os
import re
import shutil
import argparse
import datetime
from tqdm import tqdm
import pandas as pd
import pdfplumber

import config
from utils import *
from schemas import dart
from sqlalchemy import create_engine, select, delete, insert, update
from minio import Minio
from minio.error import S3Error


# --------------------------------------------------------Database Setting--------------------------------------------------------
# Create Engine
engine = create_engine(f'postgresql://{config.user}:{config.pw}@{config.host}:{config.port}/{config.db}')
client = Minio(config.minio_api_endpoint, access_key=config.user, secret_key=config.pw, secure=False)

def get_insert_query(path, company_name, year, quarter, correction, date):
    insert_query = insert(dart).values(
        path = path,
        organization = 'DART',
        name = company_name,
        year = year,
        quarter = quarter,
        correction = correction,
        documentdate = date
    )

    return insert_query

def iterate_directory(root_directory):
    file_list = []
    for root, directories, files in os.walk(root_directory):
        for filename in files:
            file_path = os.path.join(root, filename)
            file_list.append(file_path)
    return sorted(file_list)

# 폴더를 통째로 MinIO에 업로드
def upload_directory_to_minio(bucket_name, minio_path, local_path):
    assert os.path.isdir(local_path), f"Directory {local_path} doesn't exist"

    for local_file in iterate_directory(local_path):
        remote_path = os.path.join(minio_path, local_file[1 + len(local_path):])
        client.fput_object(bucket_name, remote_path, local_file)


# --------------------------------------------------------Metadata 추출--------------------------------------------------------
def match_date(s, e):
    if s.year != e.year:
        # print(f'사업연도가 다릅니다. ({s} ~ {e})')
        return None, None
    
    # 분기보고서 (Q1)
    elif s.month == 1 and s.day == 1 and e.month == 3 and e.day == 31:
        return s.year, 1
    
    # 반기보고서 (Q2)
    elif s.month == 1 and s.day == 1 and e.month == 6 and e.day == 30:
        return s.year, 2
    
    # 분기보고서 (Q3)
    elif s.month == 1 and s.day == 1 and e.month == 9 and e.day == 30:
        return s.year, 3
    
    # 사업보고서 (Q4)
    elif s.month == 1 and s.day == 1 and e.month == 12 and e.day == 31:
        return s.year, 4
    
    else:
        # print(f'분기를 식별할 수 없습니다. ({s} ~ {e})')
        return None, None

def check_duplicate(filename):
    # filename 형식: [삼성전자]사업보고서(2023.03.07)

    if len(filename.split()) > 1 and re.match(r"\(\d+\)", filename.split()[-1]): # 파일명 맨 뒤에 (1), (2) 등이 붙으면 True
        return True
    else:
        return False
    
def file_name_parser(filename):
    # filename 형식: [삼성전자]사업보고서(2023.03.07)

    duplicate = False # ex. [기아][정정]사업보고서(2021.12.10) (1)
    correction = 0 # ex. [하나금융지주][정정]사업보고서(2021.03.18)
    
    if check_duplicate(filename):
        duplicate = True
        filename = filename.split()[0]

    if '[정정]' in filename:
        correction = 1
        filename = filename.replace('[정정]', '')

    match = re.search(r"\[(.*?)\](.*?)\((.*?)\)", filename)
    
    if match:
        company_name = match.group(1)  # ex. '현대모비스'
        report_type = match.group(2)   # ex. '사업보고서'
        date = match.group(3)         # ex. '2021.03.26'
        
        if report_type in ['사업보고서', '반기보고서', '분기보고서']:
            return company_name, report_type, date, correction, duplicate
        
        else:
            # print(f'정기공시 문서가 아닙니다. ({filename})')
            return None, None, None, None, None
    
    else:
        # print(f'파일명 매칭 에러 ({filename})')
        return None, None, None, None, None

def extract_metadata(pdf_path):
    # 메타데이터를 올바르게 추출하지 못한 경우 False return

    search_scope_no_correction = 10 # 정정 문서가 아닐 때 사업연도를 탐색할 최대 페이지 수
    
    start_date_patterns = [r'(\d{4})년 (\d{2})월 (\d{2})일 부터', r'(\d{4}).(\d{2}).(\d{2}) 부터']
    end_date_patterns = [r'(\d{4})년 (\d{2})월 (\d{2})일 까지', r'(\d{4}).(\d{2}).(\d{2}) 까지']

    filename = pdf_path.split('/')[-1].split('.pdf')[0] # [삼성전자]사업보고서(2023.03.07) 형식
    company_name, report_type, date, correction, duplicate = file_name_parser(filename)

    if company_name != None: # company_name 자리에 윗줄에서 받은 5가지 중 아무거나 사용해도 됨
        date = datetime.date(int(date.split('.')[0]), int(date.split('.')[1]), int(date.split('.')[2]))

        try:
            with pdfplumber.open(pdf_path) as pdf:
                start_date = None
                end_date = None

                if correction == 0:
                    search_scope = min(search_scope_no_correction, len(pdf.pages)-1)
                else:
                    search_scope = len(pdf.pages)-1

                for p in range(search_scope):
                    # 사업연도가 나오는 페이지 탐색
                    # 첫줄에 '.'만 나오고 다음 줄에 '사 업 보 고 서' 등이 나오는 경우도 있음 (오기로 인해)
                    # len(pdf.pages[p].extract_text_lines())이 1인 경우도 있음
                    if pdf.pages[p].extract_text_lines()[0]['text'] in ['사 업 보 고 서', '반 기 보 고 서', '분 기 보 고 서'] \
                    or pdf.pages[p].extract_text_lines()[min(1, len(pdf.pages[p].extract_text_lines())-1)]['text'] in ['사 업 보 고 서', '반 기 보 고 서', '분 기 보 고 서']:
                        for line in pdf.pages[p].extract_text_lines():
                            start_date_match_1 = re.search(start_date_patterns[0], line['text'])
                            start_date_match_2 = re.search(start_date_patterns[1], line['text'])
                            end_date_match_1 = re.search(end_date_patterns[0], line['text'])
                            end_date_match_2 = re.search(end_date_patterns[1], line['text'])

                            if start_date_match_1:
                                year, month, day = start_date_match_1.groups()
                                start_date = datetime.date(int(year), int(month), int(day))
                            elif start_date_match_2:
                                year, month, day = start_date_match_2.groups()
                                start_date = datetime.date(int(year), int(month), int(day))

                            if end_date_match_1:
                                year, month, day = end_date_match_1.groups()
                                end_date = datetime.date(int(year), int(month), int(day))
                            elif end_date_match_2:
                                year, month, day = end_date_match_2.groups()
                                end_date = datetime.date(int(year), int(month), int(day))

                            if start_date != None and end_date != None:
                                # print(f'{p} 페이지에서 사업연도 정보 찾음')
                                break
                        break

                year, quarter = match_date(start_date, end_date)
                if year == None and quarter == None:
                    return False, False, False, False, False, False, False, False

            return filename, company_name, year, quarter, date, report_type, correction, duplicate

        except Exception as e:
            # print(f'{filename} 에러 발생: {e}')
            return False, False, False, False, False, False, False, False
        
    else:
        return False, False, False, False, False, False, False, False


# --------------------------------------------------------Object--------------------------------------------------------
def page_to_table_object(page):
    return page.find_tables(table_settings={'vertical_strategy': 'lines', 'horizontal_strategy': 'lines'})

def page_to_image_object(page):
    return page.images
    
def page_to_text_object(page):
    return page.extract_words()

def table_object_to_bbox(table_objects):
    return [i.bbox for i in table_objects]

def image_object_to_bbox(img_objects, page):
    return [(image['x0'], page.height - image['y1'], image['x1'], page.height - image['y0']) for image in img_objects]

def table_extractor(table_objects, im):
    if table_objects:
        table_bbox_list = [i.bbox for i in table_objects]
        # print(j, 'page Table', table_bbox_list)
        for bbox in bbox_padding(table_bbox_list):
            im.draw_rect(bbox, fill=(255, 0, 0, 30))
    return im

def image_extractor(img_objects, im, page):
    if img_objects:
        img_bbox_list = [(image['x0'], page.height - image['y1'], image['x1'], page.height - image['y0']) for image in img_objects]
        # print(j, 'page Image', img_bbox_list)
        for bbox in bbox_padding(img_bbox_list):
            im.draw_rect(bbox, fill=(255, 255, 0, 30))
    return im

def caption_extractor_table(im, table_object, text_object, threshold_caption_image=100, threshold_caption_table=30, threshold_chunk=45, threshold_line_1=11, threshold_line_2=21, bbox=False):
    '''
    image_object, table_object, text_object: pdfplumber로 추출한 image, table, text 정보
    threshold_caption_image: 해당 텍스트가 특정 키워드를 포함하는 경우 표/이미지의 캡션에 해당하는지 아닌지 구분하는 threshold (거리) (image에 대한 캡션 추출 시)
    threshold_caption_table: 해당 텍스트가 특정 키워드를 포함하는 경우 표/이미지의 캡션에 해당하는지 아닌지 구분하는 threshold (거리) (table에 대한 캡션 추출 시)
    threshold_chunk: 같은 줄에 있을 때 같은 chunk인지 아닌지 구분하는 threshold (거리)
    threshold_line_1: 한 문장이 다음 줄로 이어지는 것인지 아닌지 구분하는 threshold (y값 거리)
    threshold_line_2: 지금까지 인식한 마지막 캡션의 다음 텍스트 토큰이 특정 키워드를 포함하는 경우 해당 토큰이 이어지는 캡션인지 아닌지 구분하는 threshold (y값 거리)
    '''
    result = []

    # table에 대한 캡션 추출
    if len(table_object) != 0:
        table_data = table_object
        text_data = text_object
        table_bb_to_draw = []
        t_text_bb_to_draw = []
        t_text_contents = []

        for table in table_data:
            table_bb_to_draw.append(table.bbox)
            caption_bb_for_this_table = []
            caption_text_for_this_table = []

            i = 0
            while i < len(text_data):
                # 조건 1: 특정 키워드를 포함 (표 위/아래에 있는 경우 구분) + table과의 거리가 threshold_caption_table 이내 + 텍스트가 표에 포함되지 않음 (캡션의 시작점)
                # 표 위 정규표현식: (kk기준일 / (nnnn.
                # 표 아래 정규표현식: 주n) / *n / e)
                if rect_distance(table.bbox, get_bbox(text_data[i])) <= threshold_caption_table \
                and ((top_or_bottom(table.bbox, get_bbox(text_data[i])) == -1 and any(x in text_data[i]['text'] for x in ['[', '<', '단위', '(당기'])) \
                or (top_or_bottom(table.bbox, get_bbox(text_data[i])) == -1 and re.search("^\([가-힣]*기준일|^\([0-9]{4}\.", text_data[i]['text'])) \
                or (top_or_bottom(table.bbox, get_bbox(text_data[i])) == 1 and any(x in text_data[i]['text'] for x in ['※', '■', '☞', '[', '출처'])) \
                or (top_or_bottom(table.bbox, get_bbox(text_data[i])) == 1 and re.search("^주[0-9]*\)|\*[0-9]*|^[a-z]\)", text_data[i]['text']))) \
                and contains(table.bbox, get_bbox(text_data[i])) == False:
                    # print('condition 1: detected start of caption')
                    caption_bb_for_this_table.append(get_bbox(text_data[i]))
                    caption_text_element = []
                    caption_text_element.append(text_data[i]['text'])
                    i += 1

                    while True:
                        # 조건 2: 앞 토큰과 같은 줄 + 거리가 threshold_chunk 이내 + 텍스트가 표에 포함되지 않음
                        if get_bbox(text_data[i])[1] == get_bbox(text_data[i-1])[1] and get_bbox(text_data[i])[3] == get_bbox(text_data[i-1])[3] \
                        and rect_distance(get_bbox(text_data[i]), get_bbox(text_data[i-1])) <= threshold_chunk \
                        and contains(table.bbox, get_bbox(text_data[i])) == False:
                            # print('condition 2 - same line, same chunk')
                            caption_bb_for_this_table.append(get_bbox(text_data[i]))
                            caption_text_element.append(text_data[i]['text'])
                            i += 1

                        # 조건 3: 줄이 바뀌지만 높이 차이가 threshold_line_1 이내 + 텍스트가 표에 포함되지 않음 (이어지는 문장으로 판단) 
                        elif diff_height(get_bbox(text_data[i]), get_bbox(text_data[i-1])) <= threshold_line_1 \
                        and not (get_bbox(text_data[i])[1] == get_bbox(text_data[i-1])[1] and get_bbox(text_data[i])[3] == get_bbox(text_data[i-1])[3]) \
                        and contains(table.bbox, get_bbox(text_data[i])) == False:
                            # print('condition 3 - different line, same chunk')
                            caption_bb_for_this_table.append(get_bbox(text_data[i]))
                            caption_text_element.append(text_data[i]['text'])
                            i += 1

                        # 조건 4: 줄이 바뀌고 바로 다음 텍스트가 특정 정규표현식과 매치 + 높이 차이가 threshold_line_2 이내 + 텍스트가 표에 포함되지 않음
                        # 정규표현식: 주n) / *n / e)
                        elif re.search("^주[0-9]*\)|\*[0-9]*|^[a-z]\)", text_data[i]['text']) \
                        and diff_height(get_bbox(text_data[i]), get_bbox(text_data[i-1])) <= threshold_line_2 \
                        and not (get_bbox(text_data[i])[1] == get_bbox(text_data[i-1])[1] and get_bbox(text_data[i])[3] == get_bbox(text_data[i-1])[3]) \
                        and contains(table.bbox, get_bbox(text_data[i])) == False:
                            # print('condition 4 - different line, still caption')
                            caption_bb_for_this_table.append(get_bbox(text_data[i]))
                            caption_text_element.append(text_data[i]['text'])
                            i += 1

                        else:
                            # print('end of caption')
                            break

                    caption_text_for_this_table.append(caption_text_element)
                    
                else:
                    i += 1

            t_text_bb_to_draw.append(caption_bb_for_this_table)
            t_text_contents.append(caption_text_for_this_table)

    # 결과 저장
    if len(table_object) != 0:
        for j in range(len(table_bb_to_draw)):
            if bbox == True:
                # im.draw_rect(table_bb_to_draw[j], fill=(255, 0, 0, 30))
                im.draw_rects(t_text_bb_to_draw[j])
            # print(f'(p.{p+1}) caption for table:', [' '.join(each_caption) for each_caption in t_text_contents[j]])
            result.append([' '.join(each_caption) for each_caption in t_text_contents[j]])

    if bbox == True:
        return im, result
    else:
        return result

def caption_extractor_image(im, image_object, text_object, threshold_caption_image=100, threshold_caption_table=30, threshold_chunk=45, threshold_line_1=11, threshold_line_2=21, bbox=False):
    '''
    image_object, table_object, text_object: pdfplumber로 추출한 image, table, text 정보
    threshold_caption_image: 해당 텍스트가 특정 키워드를 포함하는 경우 표/이미지의 캡션에 해당하는지 아닌지 구분하는 threshold (거리) (image에 대한 캡션 추출 시)
    threshold_caption_table: 해당 텍스트가 특정 키워드를 포함하는 경우 표/이미지의 캡션에 해당하는지 아닌지 구분하는 threshold (거리) (table에 대한 캡션 추출 시)
    threshold_chunk: 같은 줄에 있을 때 같은 chunk인지 아닌지 구분하는 threshold (거리)
    threshold_line_1: 한 문장이 다음 줄로 이어지는 것인지 아닌지 구분하는 threshold (y값 거리)
    threshold_line_2: 지금까지 인식한 마지막 캡션의 다음 텍스트 토큰이 특정 키워드를 포함하는 경우 해당 토큰이 이어지는 캡션인지 아닌지 구분하는 threshold (y값 거리)
    '''
    result = []

    # image에 대한 캡션 추출
    if len(image_object) != 0:
        image_data = image_object
        text_data = text_object
        image_bb_to_draw = []
        i_text_bb_to_draw = []
        i_text_contents = []

        for image in image_data:
            image_bb_to_draw.append(get_bbox(image))
            caption_bb_for_this_image = []
            caption_text_for_this_image = []

            i = 0
            while i < len(text_data):
                # 조건 1: 특정 키워드를 포함 (표 위/아래에 있는 경우 구분) + image와의 거리가 threshold_caption_image 이내 + 텍스트가 표에 포함되지 않음 (캡션의 시작점)
                # 표 위 정규표현식: (kk기준일 / (nnnn.
                # 표 아래 정규표현식: 주n) / *n / e)
                if rect_distance(get_bbox(image), get_bbox(text_data[i])) <= threshold_caption_image \
                and ((top_or_bottom(get_bbox(image), get_bbox(text_data[i])) == -1 and any(x in text_data[i]['text'] for x in ['[', '<', '단위', '(당기'])) \
                or (top_or_bottom(get_bbox(image), get_bbox(text_data[i])) == -1 and re.search("^\([가-힣]*기준일|^\([0-9]{4}\.", text_data[i]['text'])) \
                or (top_or_bottom(get_bbox(image), get_bbox(text_data[i])) == 1 and any(x in text_data[i]['text'] for x in ['※', '■', '☞', '[', '출처'])) \
                or (top_or_bottom(get_bbox(image), get_bbox(text_data[i])) == 1 and re.search("^주[0-9]*\)|\*[0-9]*|^[a-z]\)", text_data[i]['text']))) \
                and contains(get_bbox(image), get_bbox(text_data[i])) == False:
                    # print('condition 1: detected start of caption')
                    caption_bb_for_this_image.append(get_bbox(text_data[i]))
                    caption_text_element = []
                    caption_text_element.append(text_data[i]['text'])
                    i += 1

                    while True:
                        # 조건 2: 앞 토큰과 같은 줄 + 거리가 threshold_chunk 이내 + 텍스트가 표에 포함되지 않음
                        if get_bbox(text_data[i])[1] == get_bbox(text_data[i-1])[1] and get_bbox(text_data[i])[3] == get_bbox(text_data[i-1])[3] \
                        and rect_distance(get_bbox(text_data[i]), get_bbox(text_data[i-1])) <= threshold_chunk \
                        and contains(get_bbox(image), get_bbox(text_data[i])) == False:
                            # print('condition 2 - same line, same chunk')
                            caption_bb_for_this_image.append(get_bbox(text_data[i]))
                            caption_text_element.append(text_data[i]['text'])
                            i += 1

                        # 조건 3: 줄이 바뀌지만 높이 차이가 threshold_line_1 이내 + 텍스트가 표에 포함되지 않음 (이어지는 문장으로 판단) 
                        elif diff_height(get_bbox(text_data[i]), get_bbox(text_data[i-1])) <= threshold_line_1 \
                        and not (get_bbox(text_data[i])[1] == get_bbox(text_data[i-1])[1] and get_bbox(text_data[i])[3] == get_bbox(text_data[i-1])[3]) \
                        and contains(get_bbox(image), get_bbox(text_data[i])) == False:
                            # print('condition 3 - different line, same chunk')
                            caption_bb_for_this_image.append(get_bbox(text_data[i]))
                            caption_text_element.append(text_data[i]['text'])
                            i += 1

                        # 조건 4: 줄이 바뀌고 바로 다음 텍스트가 특정 정규표현식과 매치 + 높이 차이가 threshold_line_2 이내 + 텍스트가 표에 포함되지 않음
                        # 정규표현식: 주n) / *n / e)
                        elif re.search("^주[0-9]*\)|\*[0-9]*|^[a-z]\)", text_data[i]['text']) \
                        and diff_height(get_bbox(text_data[i]), get_bbox(text_data[i-1])) <= threshold_line_2 \
                        and not (get_bbox(text_data[i])[1] == get_bbox(text_data[i-1])[1] and get_bbox(text_data[i])[3] == get_bbox(text_data[i-1])[3]) \
                        and contains(get_bbox(image), get_bbox(text_data[i])) == False:
                            # print('condition 4 - different line, still caption')
                            caption_bb_for_this_image.append(get_bbox(text_data[i]))
                            caption_text_element.append(text_data[i]['text'])
                            i += 1

                        else:
                            # print('end of caption')
                            break

                    caption_text_for_this_image.append(caption_text_element)
                    
                else:
                    i += 1

            i_text_bb_to_draw.append(caption_bb_for_this_image)
            i_text_contents.append(caption_text_for_this_image)

    # 결과 저장   
    if len(image_object) != 0:
        for j in range(len(image_bb_to_draw)):
            if bbox == True:
                # im.draw_rect(image_bb_to_draw[j], fill=(255, 255, 0, 30))
                im.draw_rects(i_text_bb_to_draw[j])
            # print(f'(p.{p+1}) caption for image:', [' '.join(each_caption) for each_caption in i_text_contents[j]])
            result.append([' '.join(each_caption) for each_caption in i_text_contents[j]])

    if bbox == True:
        return im, result
    else:
        return result


# --------------------------------------------------------Main--------------------------------------------------------
def main(args):

    # make save directory
    if not os.path.exists(args.save_dir):
        os.makedirs(args.save_dir)
    
    pdf_paths = get_file_paths(args.pdf_dir)
    pdf_names = get_file_names(args.pdf_dir)

    # loop each pdf
    for doc_no, (pdf_name, pdf_path) in enumerate(zip(tqdm(pdf_names), pdf_paths)):
        # print(f'[{doc_no+1}/{len(pdf_names)}] Processing {pdf_name}')

        # 메타데이터 추출 & DB에 적재 (PostgreSQL)
        if args.save_db:
            filename, company_name, year, quarter, date, report_type, correction, duplicate = extract_metadata(pdf_path)
            newerExists = False

            # 메타데이터가 올바르게 추출된 경우
            if filename is not False:
                with engine.connect() as con:
                    transactions = con.begin()

                    try:
                        objects = ['text', 'table', 'image', 'caption']
                        if args.page_bbox:
                            objects.append('page_bbox')

                        for object in objects: # 파일로 하나 폴더로 하나 상관 없나? 이 path column의 역할 알
                            # print('object:', object)
                            path = f'corporate-finance/DART/{company_name}/{year}/Q{quarter}/{object}'
                            
                            # 기존 SQL Table에 동일한 문서에 대한 기록이 있는지 체크
                            serach_query = select(dart).where(
                                dart.c.path == path, # path 이용해서 object 종류 체크
                                dart.c.organization == 'DART', 
                                dart.c.name == company_name, 
                                dart.c.year == year, 
                                dart.c.quarter == quarter)
                            result = con.execute(serach_query).fetchall() # Query 결과를 리스트에 저장

                            # 기존 SQL table에 동일한 문서에 대한 기록이 존재하지 않는 경우
                            # insert
                            if len(result) == 0:
                                # print('Case 1: Insert')
                                insert_query = get_insert_query(path, company_name, year, quarter, correction, date)
                                con.execute(insert_query)
                                # transactions.commit()

                            # 기존 SQL table에 동일한 문서에 대한 더 오래된 기록이 존재하는 경우
                            # insert 대신 update
                            elif len(result) == 1 and result[0].documentdate < date:
                                # print('Case 2: Update')
                                update_query = update(dart).where(
                                    dart.c.path == path,
                                    dart.c.organization == 'DART', 
                                    dart.c.name == company_name, 
                                    dart.c.year == year, 
                                    dart.c.quarter == quarter).values(
                                    documentdate = date,
                                    correction = correction)
                                con.execute(update_query)
                                # transactions.commit()

                            # 기존 SQL table에 동일한 문서에 대한 더 최신 (혹은 동일한) 기록이 존재하는 경우
                            # SQL, MinIO 둘 다 업데이트 x
                            elif len(result) == 1 and result[0].documentdate >= date:
                                # print('Case 3: Newer or same exists')
                                newerExists = True # 뒤에서 MinIO 다룰 때 사용
                                
                            elif len(result) > 1:
                                # print('Case 4 : Multiple rows exist')
                                exit('Multiple rows exist about the same document in the current SQL table') # 제대로 작동하나? 감싸고 있는 try-except랑 같이

                        transactions.commit()

                    except Exception as e:
                        print(f'\nError occured while inserting/updating metadata of {pdf_name}') # 잘 되나 확인
                        print(f'Error: {e}')

                        transactions.rollback()

            else:
                print(f'\nFailed to extract metadata from {pdf_name}. Data will not be uploaded to DB.')

        try:
            # make directory for each pdf
            save_path = os.path.join(args.save_dir, pdf_name[:-4])
            if not os.path.exists(save_path):
                os.mkdir(save_path)

            # make directory for each object
            text_dir = os.path.join(save_path, 'text')
            table_dir = os.path.join(save_path, 'table')
            image_dir = os.path.join(save_path, 'image')
            caption_dir = os.path.join(save_path, 'caption')

            for dir in [text_dir, table_dir, image_dir, caption_dir]:
                if not os.path.exists(dir):
                    os.mkdir(dir)

            if args.page_bbox:
                bbox_dir = os.path.join(save_path, 'page_bbox')
                if not os.path.exists(bbox_dir):
                    os.mkdir(bbox_dir)

            # define pdf object
            pdf = pdfplumber.open(pdf_path)
            pages = pdf.pages # define each page

            # list to store rows for df
            df_text_rows = []
            df_caption_rows = []

            # loop each page
            for i, page in enumerate(pages):
            
                table_objects = page_to_table_object(page)
                image_objects = page_to_image_object(page)
                text_objects = page_to_text_object(page)

                if args.page_bbox == True:
                    im = page.to_image(resolution=args.resolution_page)

                # extract text
                only_text = [''.join(text_object['text']) for text_object in text_objects]
                df_text_rows.append([i+1, ' '.join(only_text)]) # page | text

                # with open(os.path.join(save_path, 'text', f'page{i+1}.txt'), 'w', encoding='UTF-8') as f:
                #     f.write(' '.join(only_text))

                # extract table
                table_bboxs = table_object_to_bbox(table_objects)
                for table_num, table_bbox in enumerate(table_bboxs):
                    crop_table_im = page.crop(table_bbox).to_image(resolution=args.resolution_object)
                    crop_table_im.save(os.path.join(save_path, 'table', f'page{i+1}_table{table_num+1}.png'))

                # extract image
                image_bboxs = image_object_to_bbox(image_objects, page)
                for image_num, image_bbox in enumerate(image_bboxs):
                    crop_image_im = page.crop(image_bbox).to_image(resolution=args.resolution_object)
                    crop_image_im.save(os.path.join(save_path, 'image', f'page{i+1}_image{image_num+1}.png'))

                # extract caption (table)
                if args.page_bbox == True:
                    im, caption_info_table = caption_extractor_table(im, table_objects, text_objects, bbox=True)
                else:
                    caption_info_table = caption_extractor_table(None, table_objects, text_objects, bbox=False)
                
                for table_num, table_txt in enumerate(caption_info_table):
                    df_caption_rows.append([i+1, f'table{table_num+1}', '\n'.join(table_txt)]) # page | object | caption

                    # with open(os.path.join(save_path, 'caption', f'page{i+1}_table{table_num+1}.txt'), 'w', encoding='UTF-8') as f:
                    #     f.write('\n'.join(table_txt))

                # extract caption (image)
                if args.page_bbox == True:
                    im, caption_info_image = caption_extractor_image(im, image_objects, text_objects, bbox=True)
                else:
                    caption_info_image = caption_extractor_image(None, image_objects, text_objects, bbox=False)

                for image_num, image_txt in enumerate(caption_info_image):
                    df_caption_rows.append([i+1, f'image{image_num+1}', '\n'.join(image_txt)]) # page | object | caption

                    # with open(os.path.join(save_path, 'caption', f'page{i+1}_image{image_num+1}.txt'), 'w', encoding='UTF-8') as f:
                    #     f.write('\n'.join(image_txt))

                # save page bounding box image with extracted object
                if args.page_bbox == True:
                    im = table_extractor(table_objects, im)
                    im = image_extractor(image_objects, im, page)
                    im.save(os.path.join(save_path, 'page_bbox', f'page{i+1}.png'), format='PNG')

            # text/caption: df 만들어서 parquet 파일로
            df_text = pd.DataFrame(df_text_rows, columns = ['page', 'text'])
            df_caption = pd.DataFrame(df_caption_rows, columns = ['page', 'object', 'caption'])

            df_text.to_parquet(f'{save_path}/text/text.parquet')
            df_caption.to_parquet(f'{save_path}/caption/caption.parquet')

            # 추출한 object DB에 적재 (MinIO)
            if args.save_db:
                if filename is not False and newerExists is False:
                    upload_directory_to_minio('corporate-finance', f'DART/{company_name}/{year}/Q{quarter}', save_path)

            # save_local 선택 안 한 경우는 로컬에서 결과 폴더 삭제
            if not args.save_local:
                if os.path.exists(save_path):
                    shutil.rmtree(save_path)

        except Exception as e:
            print(f'\nError occured while extracting objects from {pdf_name}. Skip this document.')
            print(f'Error: {e}')
            # 에러 발생한 파일은 로컬/DB에 둘 다 결과물 저장 x
            if os.path.exists(save_path):
                shutil.rmtree(save_path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--pdf_dir', type=str, default='input')
    parser.add_argument('--save_dir', type=str, default='output')
    parser.add_argument('--page_bbox', action='store_true', default=False)
    parser.add_argument('--resolution_object', type=int, default=200)
    parser.add_argument('--resolution_page', type=int, default=200)
    parser.add_argument('--save_local', action='store_true', default=False)
    parser.add_argument('--save_db', action='store_true', default=False)
    args = parser.parse_args()

    assert args.save_local or args.save_db, 'You have to save the results in at least one device'

    main(args)
