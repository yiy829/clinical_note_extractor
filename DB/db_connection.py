import sys
import os
import traceback

import psycopg2 as pg
import pandas.io.sql as psql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects import mysql

from DB import db_config

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname('../../..'))))

'''
    프로그램 기능 : 서버 내 DB와 연동하여 데이터 조회, 삽입 등을 처리하는 함수 모음

    @author : yiy829
    @create : 2022. 04. 19
    @update : 2022. 04. 19

'''
# DB정보 불러오기
db_name = str(db_config.DB_NAME)
host = str(db_config.DB_HOST)
port = str(db_config.DB_PORT)
user = str(db_config.DB_USER)
passwd = str(db_config.DB_PASSWORD)


'''
    DB와 연동하여 데이터 베이스에 있는 테이블에 쿼리를 보내 데이터를 가져오는 함수(DB SELECT)

    @param : 
        1. p_str_db_name : [문자열] 데이터베이스 이름
        2. p_str_sql_query : [문자열] SQL SELECT 쿼리문
    @return : 
        1. df_result : [데이터프레임] 조회한 결과문을 반환
'''
def fn_select_query(p_str_db_name=db_name, p_str_sql_query=''):
    conn = pg.connect(host=host, port=int(port), user=user, password=passwd,
                           database=p_str_db_name)
    conn.set_client_encoding('UTF8')
    conn.set_session(autocommit=True)

    try:
        df_result = psql.read_sql(p_str_sql_query, conn)
        # print("fn_select_query : DB 조회 완료")

        return df_result
    except:
        traceback.print_exc()
    finally:
        conn.close()


'''
    DB와 연동하여 데이터 프레임을 table에 넣는 함수(DB Insert)

    @param : 
        1. p_df_data : [데이터프레임] 원본 데이터 셋
        2. p_str_db_name : [문자열] 데이터베이스 이름
        3. p_str_table_name : [문자열] 데이터베이스 내 테이블 이름
        4. p_str_if_exists : 'append' == '데이터 추가', 'replace' == 테이블 드랍 후 데이터 추가, 'fail' == 테이블 존재 시 에러 발생
    @return : 
        -
'''
def fn_df_insert_db(p_df_data, p_str_db_name=db_name, p_str_table_name='', p_str_if_exists='append'):
    local_engine = create_engine(
        "postgresql://" + user + ":" + passwd + "@" + host + ":" + port + "/" + p_str_db_name,
        encoding='utf-8')

    conn = local_engine.connect()

    try:
        # p_str_if_exists 값이 replace 일 경우, 테이블을 삭제하지 않고 truncate 후, 데이터 Insert
        if p_str_if_exists == 'replace':
            str_truncate_query = "TRUNCATE TABLE " + p_str_table_name
            conn.execute(str_truncate_query)
            print(p_str_table_name, " TRUNCATE 완료")
            str_if_exists = "append"
        else:
            str_if_exists = p_str_if_exists

        if len(p_df_data) == 0:
            print("Error 데이터 없음!!!!")
        else:
            # if_exists = 'append' / if_exists = 'fail' / if_exists = 'replace'
            # p_b_dtype = True 일 경우 모든 컬럼의 타입을 MEDIUMTEXT 사용
            try:
                p_df_data.to_sql(name=p_str_table_name, con=local_engine, if_exists=str_if_exists, index=False)
                print("fn_df_insert_db : ", p_str_table_name, " DB 삽입 완료")

            except sqlalchemy.exc.InternalError:
                try:
                    colnames = p_df_data.columns
                    dic_col_type = {}
                    for val in colnames:
                        dic_col_type[val] = mysql.MEDIUMTEXT()

                    p_df_data.to_sql(name=p_str_table_name, con=local_engine, if_exists=str_if_exists, index=False,
                                     dtype=dic_col_type)
                    print("fn_df_insert_db : ", p_str_table_name, " DB 삽입 완료")

                except:
                    traceback.print_exc()

            except sqlalchemy.exc.DataError:
                try:
                    colnames = p_df_data.columns
                    dic_col_type = {}
                    for val in colnames:
                        dic_col_type[val] = mysql.MEDIUMTEXT()

                    p_df_data.to_sql(name=p_str_table_name, con=local_engine, if_exists=str_if_exists, index=False,
                                     dtype=dic_col_type)
                    print("fn_df_insert_db : ", p_str_table_name, " DB 삽입 완료")

                except:
                    traceback.print_exc()
    except:
        traceback.print_exc()
    finally:
        conn.close()


'''
    DB와 연동하여 테이블을 생성하는 함수

    @param : 
        1. p_str_db_name : [문자열] 데이터베이스 이름
        2. p_str_create_table_query : [문자열] SQL TABLE 생성 쿼리문
'''
def fn_create_table(p_str_db_name=db_name, p_str_create_table_query=''):
    local_engine = create_engine(
        "postgresql://" + user + ":" + passwd + "@" + host + ":" + port + "/" + p_str_db_name,
        encoding='utf-8')
    conn = local_engine.connect()
    # conn.set_client_encoding('UTF8')

    try:
        conn.execute(p_str_create_table_query)

        str_table_name = p_str_create_table_query.split("TABLE")[1].split("(")[0]

        print(str_table_name, "TABLE 생성 완료")

    except:
        traceback.print_exc()
    finally:
        conn.close()


'''
    운영 서버 내 DB와 연동하여 INSERT 쿼리문을 받아서 적용하는 함수
'''
def fn_insert_table(p_str_db_name=db_name, p_str_insert_query=''):
    local_engine = create_engine(
        "postgresql://" + user + ":" + passwd + "@" + host + ":" + port + "/" + p_str_db_name,
        encoding='utf-8')
    conn = local_engine.connect()

    try:
        str_insert_query = p_str_insert_query

        conn.execute(str_insert_query)

        # print("Insert 완료")
    except:
        traceback.print_exc()
    finally:
        conn.close()

