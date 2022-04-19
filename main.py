import warnings

from Extractor import insert_data

# append FutureWarning 및 SQLAlchemy UserWarning 콘솔뜨는 현상 제거
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

# DB입력 메인 함수
if __name__ == '__main__':
    insert_data.InsertDataHandler().fn_main_insert_data()