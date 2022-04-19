# clinical_note_extractor
- 해당 모듈은 의료진단 문서를 처리하여 DB에 입력하는 모듈입니다.

## 1. 동작 방법
1) requirements.txt를 통해 패키지를 설치해줍니다.
2) 프로젝트/DB/db_config.py 경로에 DB 정보를 채워줍니다.(전체 문자열로)
3) main.py 파일을 실행시켜줍니다.

## 2. 주의 사항
1) 해당 모듈에서 제가 수행했던 user 정보로 config파일을 수정해야 합니다. 아닐 경우 DB 입력 시, cofing파일에 입력한 user와 같은 명의 스키마로 INSERT될 수 있습니다.
2) 시간이 모자라 스키마안에 테이블을 truncate하는 기능을 추가하지 못하였습니다. 기본으로 테이블에 데이터를 append하는 식으로 되어있으니, 참고부탁드립니다.
