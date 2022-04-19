from DB import db_config, db_connection

'''
    프로그램 기능 : DB를 통해 원본 문서 데이터를 불러와 공통적인 데이터 처리를 하는 기능

    @author : yiy829
    @create : 2022. 04. 19
    @update : 2022. 04. 19
'''
class LoadDataHandler():
    def __init__(self):
        self.num = ''

    '''
        DB와 연동하여 원본 데이터 불러오기
    
        @param : 
            -
        @return : 
            1. clinical_note : [데이터프레임] 'clinical_note' 테이블 원본 데이터
    '''
    def fn_load_origin_data(self):
        try:
            clinical_note = db_connection.fn_select_query(p_str_sql_query=db_config.SELECT_CLINICAL_NOTE)
        except Exception as e:
            print(e)

        return clinical_note

    '''
        불러온 원본 데이터 한 row씩 전처리하는 기능
    
        @param : 
            1. p_input_list : [리스트] clinical_note에서 한 행의 본문('note'컬럼) 데이터
        @return : 
            1. split_list : [리스트] 불필요한 문자 제거 및 개행단위로 본문을 나눈 리스트
    '''
    def fn_preprocess_origin_data(self, p_split_list):
        try:
            # 'CONTINUING' 이후 제거
            remove_index = [i for i in range(len(p_split_list)) if "CONTINUING" in p_split_list[i]]
            split_list = p_split_list[0:remove_index[0]]

        except Exception as e:
            print(e)

        return split_list


