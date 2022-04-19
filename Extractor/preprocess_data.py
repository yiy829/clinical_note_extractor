import random

from DB import db_config, db_connection

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
            3. rand_num : [정수] 중복확인한 1~8자리의 무작위 수
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
        -
    @return : 
        -
'''

'''
    visit_occurrence 테이블에 맞게 가공하는 함수

    @param : 
        -
    @return : 
        -
'''

'''
    drug_exposure 테이블에 맞게 가공하는 함수

    @param : 
        -
    @return : 
        -
'''

'''
    condition_occurrence 테이블에 맞게 가공하는 함수

    @param : 
        -
    @return : 
        -
'''

'''
    데이터 가공 메인 함수

    @param : 
        -
    @return : 
        -
'''