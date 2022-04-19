#!/usr/bin/env python
# coding: utf-8

# DB_INFO
DB_NAME = ''
DB_HOST = ''
DB_PORT = ''
DB_USER = ''
DB_PASSWORD = ''

# TABLE_LIST
INSERT_TABLE_LIST = ['person', 'visit_occurrence', 'drug_exposure', 'condition_occurrence']

# CREATE QUERY
PERSON_CREATE_QUERY = """
    CREATE TABLE walker104.person(
        person_id bigint NOT NULL
        , year_of_birth integer NULL
        , month_of_birth integer NULL
        , day_of_birth integer NULL
        , death_date timestamp NULL
        , gender_value varchar(50) NULL
        , race_value varchar(50) NULL
        , ethnicity_value varchar(50) NULL
        , CONSTRAINT xpk_person PRIMARY KEY (person_id)
    );
    """

VISIT_OCCURRENCE_CREATE_QUERY = """
    CREATE TABLE walker104.visit_occurrence(
        visit_occurrence_id bigint NOT NULL
        , person_id bigint NULL
        , visit_start_date date NULL
        , care_site_nm text NULL
        , visit_type_value varchar(50) NULL
        , CONSTRAINT xpk_visit_occurrence PRIMARY KEY (visit_occurrence_id)
    );
    """

DRUG_EXPOSURE_CREATE_QUERY = """
    CREATE TABLE walker104.drug_exposure(
        drug_exposure_id bigint NOT NULL
        , person_id bigint NULL
        , drug_exposure_start_date date NULL
        , drug_value text NULL
        , route_value varchar(50) NULL
        , dose_value varchar(50) NULL
        , unit_value varchar(50) NULL
        , visit_occurrence_id bigint NULL
        , CONSTRAINT xpk_drug_exposure PRIMARY KEY (drug_exposure_id)
    );
    """

CONDITION_OCCURRENCE_CREATE_QUERY = """
    CREATE TABLE walker104.condition_occurrence(
        condition_occurrence_id bigint NOT NULL
        , person_id bigint NULL
        , condition_start_date date NULL
        , condition_value text NULL
        , visit_occurrence_id bigint NULL
        , CONSTRAINT xpk_condition_occurrence PRIMARY KEY (condition_occurrence_id)
    );
    """

QUERY_MATCHING = {
    'person': PERSON_CREATE_QUERY
    , 'visit_occurrence': VISIT_OCCURRENCE_CREATE_QUERY
    , 'drug_exposure': DRUG_EXPOSURE_CREATE_QUERY
    , 'condition_occurrence': CONDITION_OCCURRENCE_CREATE_QUERY
}

# SELECT QUERY
SELECT_CLINICAL_NOTE = """
    SELECT 
        *
    FROM 
        de.CLINICAL_NOTE CN 
    ;
    """

CONFIRM_PK_NUM = """
    SELECT {} 
    FROM 
        {}
    WHERE
        {} = {}
    ;
    """

IS_TABLE_EXIST = """
    SELECT COUNT(*) 
    FROM 
        pg_tables 
    WHERE 
        schemaname='{}' 
    AND 
        tablename='{}'
    ; 
    """
