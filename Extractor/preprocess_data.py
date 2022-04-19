import random
import re
import pandas as pd
from pandas import DataFrame

from DB import db_config, db_connection
from Extractor import load_data

'''
    프로그램 기능 : 불러온 데이터를 특정 테이블에 맞춰 가공

    @author : yiy829
    @create : 2022. 04. 19
    @update : 2022. 04. 19
'''
class PreprocessHandler():
    def __init__(self):
        self.num = ''

    '''
        난수 생성 및 해당 수가 있는지 확인하는 함수
    
        @param : 
            1. p_str_table_name : [문자열] 조회할 테이블명
            2. p_str_col_name : [문자열] 조회할 컬럼명
        @return : 
            1. rand_num : [정수] 중복확인한 1~8자리의 무작위 수
    '''
    def fn_randomize_num(self, p_str_table_name, p_str_col_name):
        try:
            rand_num = random.randrange(1, 100000000)
            confirm_query = db_config.CONFIRM_PK_NUM.format(p_str_col_name, p_str_table_name, p_str_col_name, rand_num)
            verify_data = db_connection.fn_select_query(p_str_sql_query=confirm_query)

            # 생성한 난수가 존재하는 경우 없을때까지 반복
            while len(verify_data) != 0:
                rand_num = random.randrange(1, 100000000)
                confirm_query = db_config.CONFIRM_PK_NUM.format(p_str_col_name, p_str_table_name, p_str_col_name,
                                                                rand_num)
                verify_data = db_connection.fn_select_query(p_str_sql_query=confirm_query)
        except Exception as e:
            print(e)

        return rand_num


    '''
        person 테이블에 맞게 가공하는 함수
    
        @param : 
            1. p_split_list : [리스트] 본문 데이터에서 개행 단위로 자른 문장 리스트
        @return : 
            1. df_person : [데이터프레임] 무작위 생성 id를 제외하고 나머지가 입력된 데이터
    '''
    def fn_preprocess_person(self, p_split_list):
        try:
            df_person = pd.DataFrame(
                columns=["person_id", "year_of_birth", "month_of_birth", "day_of_birth", "death_date", "gender_value",
                         "race_value", "ethnicity_value"])

            # 인종
            race_index = [i for i in range(len(p_split_list)) if "Race:" in p_split_list[i]]
            race_value = p_split_list[race_index[0]].split("Race:")[1].replace(" ", "").lower()

            # 민족성
            ethnicity_index = [i for i in range(len(p_split_list)) if "Ethnicity:" in p_split_list[i]]
            ethnicity_value = re.sub("[' '|\-]", "", p_split_list[ethnicity_index[0]].split("Ethnicity:")[1].lower())

            # 성별
            gender_index = [i for i in range(len(p_split_list)) if "Gender:" in p_split_list[i]]
            gender_value = p_split_list[gender_index[0]].split("Gender:")[1].replace(" ", "")

            # # 나이
            # age_index = [i for i in range(len(split_list)) if "Age:" in split_list[i]]
            # age = split_list[age_index[0]].split("Age:")[1].replace(" ","")

            # 생년월일
            birth_index = [i for i in range(len(p_split_list)) if "Birth Date:" in p_split_list[i]]
            birth = p_split_list[birth_index[0]].split("Birth Date:")[1].replace(" ", "")
            year_of_birth = int(birth.split("-")[0])
            month_of_birth = int(birth.split("-")[1])
            day_of_birth = int(birth.split("-")[2])

            # 사망날짜
            death_index = [i for i in range(len(p_split_list)) if "Death Date:" in p_split_list[i]]
            death_date = None
            if len(death_index) != 0:
                death_date = p_split_list[death_index[0]].split("Death Date:")[1].replace(" ", "").lower()

            input_dict = {'year_of_birth': year_of_birth
                , 'month_of_birth': month_of_birth
                , 'day_of_birth': day_of_birth
                , 'death_date': death_date
                , 'gender_value': gender_value
                , 'race_value': race_value
                , 'ethnicity_value': ethnicity_value
                          }
            df_person = df_person.append(input_dict, ignore_index=True)

        except Exception as e:
            print(e)

        return df_person



    '''
        visit_occurrence 테이블에 맞게 가공하는 함수
    
        @param : 
            1. p_split_list : [리스트] 본문 데이터에서 개행 단위로 자른 문장 리스트
        @return : 
            1. df_visit_occurrence : [데이터프레임] 무작위 생성 id를 제외하고 나머지가 입력된 데이터
    '''
    def fn_preprocess_visit_occurrence(self, p_split_list):
        try:
            visit_occurrence = pd.DataFrame(
                columns=["visit_occurrence_id", "person_id", "visit_start_date", "care_site_nm", "visit_type_value"])

            encounter_index = [i + 1 for i in range(len(p_split_list)) if "ENCOUNTER" in p_split_list[i]]
            # 내원한 일자
            visit_start_date = p_split_list[encounter_index[0]].split(":")[0].replace(" ", "")
            # 내원한 기관명
            care_site_nm = p_split_list[encounter_index[0]].split(":")[1].replace(" Encounter at ", "").strip()

            visit_type_index = [i for i in range(len(p_split_list)) if "Type: " in p_split_list[i]]
            # 내원 종류에 대한 정보
            visit_type_value = p_split_list[visit_type_index[0]].split("Type: ")[1].strip()

            input_dict = {'visit_start_date': visit_start_date
                , 'care_site_nm': care_site_nm
                , 'visit_type_value': visit_type_value}
            df_visit_occurrence = visit_occurrence.append(input_dict, ignore_index=True)

        except Exception as e:
            print(e)

        return df_visit_occurrence

    '''
        drug_exposure 테이블에 맞게 가공하는 함수
    
        @param : 
            1. p_split_list : [리스트] 본문 데이터에서 개행 단위로 자른 문장 리스트
        @return : 
            1. df_drug_exposure : [데이터프레임] 무작위 생성 id를 제외하고 나머지가 입력된 데이터
    '''
    def fn_preprocess_drug_exposure(self, p_split_list):
        try:
            drug_exposure = pd.DataFrame(
                columns=["drug_exposure_id", "person_id", "drug_exposure_start_date", "drug_value", "route_value",
                         "dose_value", "unit_value", "visit_occurrence_id"])
            # drug_exposure
            drug_index = [i + 1 for i in range(len(p_split_list)) if "MEDICATIONS:" in p_split_list[i]]
            drug_history = p_split_list[drug_index[0]].split(":")

            for i in range(0, len(drug_history), 2):
                # 약처방일자
                drug_exposure_start_date = drug_history[i].strip()
                # 약 성분명
                drug_value = drug_history[i + 1].strip().split(" ")[0]
                # 약 용량 정보
                dose_value = drug_history[i + 1].strip().split(" ")[1]
                # 용량의 단위 정보
                unit_value = drug_history[i + 1].strip().split(" ")[2]
                # 약 복용경로
                route_value = drug_history[i + 1].strip().split(" ")[3]

                input_dict = {'drug_exposure_start_date': drug_exposure_start_date,
                              'drug_value': drug_value, 'dose_value': dose_value,
                              'unit_value': unit_value, 'route_value': route_value}
                df_drug_exposure = drug_exposure.append(input_dict, ignore_index=True)

        except Exception as e:
            print(e)

        return df_drug_exposure

    '''
        condition_occurrence 테이블에 맞게 가공하는 함수
    
        @param : 
            1. p_split_list : [리스트] 본문 데이터에서 개행 단위로 자른 문장 리스트
        @return : 
            1. df_condition_occurrence : [데이터프레임] 무작위 생성 id를 제외하고 나머지가 입력된 데이터
    '''
    def fn_preprocess_condition_occurrence(self, p_split_list):
        try:
            condition_index = [i + 1 for i in range(len(p_split_list)) if "CONDITIONS" in p_split_list[i]]
            condition_history = p_split_list[condition_index[0]].split(":")

            condition_occurrence = pd.DataFrame(
                columns=["condition_occurrence_id", "person_id", "condition_start_date", "condition_value",
                         "visit_occurrence_id"])
            condition_history
            for i in range(0, len(condition_history), 2):
                # 진단받은 일자
                condition_start_date = condition_history[i].strip()
                # 진단명 정보
                condition_value = condition_history[i + 1].strip()

                input_dict = {'condition_start_date': condition_start_date,
                              'condition_value': condition_value}
                df_condition_occurrence = condition_occurrence.append(input_dict, ignore_index=True)

        except Exception as e:
            print(e)

        return df_condition_occurrence

    '''
        데이터 가공 메인 함수
    
        @param : 
            -
        @return : 
            -
    '''
    def fn_preprocess_main(self):
        try:
            # 데이터프레임 생성
            person = pd.DataFrame(
                columns=["person_id", "year_of_birth", "month_of_birth", "day_of_birth", "death_date", "gender_value",
                         "race_value", "ethnicity_value"])
            visit_occurrence = pd.DataFrame(
                columns=["visit_occurrence_id", "person_id", "visit_start_date", "care_site_nm", "visit_type_value"])
            drug_exposure = pd.DataFrame(
                columns=["drug_exposure_id", "person_id", "drug_exposure_start_date", "drug_value", "route_value",
                         "dose_value", "unit_value", "visit_occurrence_id"])
            condition_occurrence = pd.DataFrame(
                columns=["condition_occurrence_id", "person_id", "condition_start_date", "condition_value",
                         "visit_occurrence_id"])

            clinical_data = load_data.LoadDataHandler().fn_load_origin_data()

            # 데이터 행별로 전처리
            for i in range(len(clinical_data)):
                split_list = load_data.LoadDataHandler().fn_preprocess_origin_data(clinical_data)
                person_row = PreprocessHandler().fn_preprocess_person(split_list)
                visit_occurrence_row = PreprocessHandler().fn_preprocess_visit_occurrence(split_list)
                drug_exposure_row = PreprocessHandler().fn_preprocess_drug_exposure(split_list)
                condition_occurrence_row = PreprocessHandler().fn_preprocess_condition_occurrence(split_list)

                # person_id 입력
                person_id = PreprocessHandler().fn_randomize_num(p_str_table_name='person',
                                                                 p_str_col_name='person_id')
                person_row['person_id'] = person_id
                visit_occurrence_row['person_id'] = person_id
                drug_exposure_row['person_id'] = person_id
                condition_occurrence_row['person_id'] = person_id

                # visit_occurrence_id 난수생성 및 조회, 입력
                visit_occurrence_id = PreprocessHandler().fn_randomize_num(p_str_table_name='visit_occurrence',
                                                                           p_str_col_name='visit_occurrence_id')
                visit_occurrence_row['visit_occurrence_id'] = visit_occurrence_id
                drug_exposure_row['visit_occurrence_id'] = visit_occurrence_id
                condition_occurrence_row['visit_occurrence_id'] = visit_occurrence_id

                # drug_exposure_id 난수생성 및 조회, 입력
                drug_exposure_id = PreprocessHandler().fn_randomize_num(p_str_table_name='drug_exposure',
                                                                        p_str_col_name='drug_exposure_id')
                drug_exposure_row['drug_exposure_id'] = drug_exposure_id

                # condition_occurrence_id 난수생성 및 조회, 입력
                condition_occurrence_id = PreprocessHandler().fn_randomize_num(p_str_table_name='condition_occurrence',
                                                                               p_str_col_name='condition_occurrence_id')
                condition_occurrence_row['condition_occurrence_id'] = condition_occurrence_id

                # 테이블 별 행 데이터 append
                person = person.append(person_row, ignore_index=True)
                visit_occurrence = visit_occurrence.append(visit_occurrence_row, ignore_index=True)
                drug_exposure = drug_exposure.append(drug_exposure_row, ignore_index=True)
                condition_occurrence = condition_occurrence.append(condition_occurrence_row, ignore_index=True)

        except Exception as e:
            print(e)

        return person, visit_occurrence, drug_exposure, condition_occurrence


