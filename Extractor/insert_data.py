from DB import db_config, db_connection
from Extractor import preprocess_data

'''
    프로그램 기능 : 가공한 데이터를 특정 테이블에 입력

    @author : yiy829
    @create : 2022. 04. 19
    @update : 2022. 04. 19
'''
class InsertDataHandler():
    def __init__(self):
        self.num = ''

    '''
       테이블 존재여부 확인 함수

       @param : 
           1. str_table_name : [문자열] 확인할 테이블 명
       @return : 
           1. answer : [불리언] 테이블이 존재할 경우 True, 않을 경우 False
    '''
    def fn_is_table_exist(self, p_str_table_name):
        try:
            is_exist = db_connection.fn_select_query(p_str_sql_query=db_config.IS_TABLE_EXIST.format(db_config.DB_USER,
                                                                                                     p_str_table_name))
            if is_exist['count'].loc[0] == 0:
                answer = False
            else:
                answer = True

        except Exception as e:
            print(e)

        return answer

    '''
       테이블별로 존재여부 확인하고 없을 경우 생성하는 함수

       @param : 
           -
       @return : 
           1. answer
    '''
    def fn_create_table(self):
        try:
            # 테이블리스트 불러오기
            table_list = db_config.INSERT_TABLE_LIST

            # 존재여부 확인
            for table in table_list:
                answer = InsertDataHandler().fn_is_table_exist(table)
                # 없을경우 테이블 생성
                if not answer:
                    db_connection.fn_create_table(p_str_create_table_query=db_config.QUERY_MATCHING[table])
                # print(table, " 생성 완료")

        except Exception as e:
            print(e)

    '''
        테이블 입력 메인 함수
    
        @param : 
            -
        @return : 
            -
    '''
    def fn_main_insert_data(self):
        try:
            # 테이블 없을 경우 생성
            InsertDataHandler().fn_create_table()

            # 처리된 데이터 불러오기
            person, visit_occurrence, drug_exposure, condition_occurrence = \
                preprocess_data.PreprocessHandler().fn_preprocess_main()

            # 처리된 데이터 테이블 별로 입력하기
            db_connection.fn_df_insert_db(p_df_data=person, p_str_table_name="person")
            db_connection.fn_df_insert_db(p_df_data=visit_occurrence, p_str_table_name="visit_occurrence")
            db_connection.fn_df_insert_db(p_df_data=drug_exposure, p_str_table_name="drug_exposure")
            db_connection.fn_df_insert_db(p_df_data=condition_occurrence, p_str_table_name="condition_occurrence")

            print("DB 입력 완료")

        except Exception as e:
            print(e)