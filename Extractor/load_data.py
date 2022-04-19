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
        불러온 원본 데이터 전처리(필요시 추가 예정)
    
        @param : 
            -
        @return : 
            -
    '''
    def fn_preprocess_origin_data(self):
        return None