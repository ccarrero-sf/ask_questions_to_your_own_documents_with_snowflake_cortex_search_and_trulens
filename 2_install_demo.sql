

USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS_TRU;
USE SCHEMA PUBLIC;

create or replace stage docs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

COPY FILES
    INTO @DOCS/
    FROM @CC_QUICKSTART_CORTEX_SEARCH_DOCS_TRU.PUBLIC.git_repo_chatbot/branches/main/docs/
    PATTERN='.*[.]pdf';


ALTER STAGE DOCS REFRESH;

select * from directory(@docs);


create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512, #Adjust this as you see fit
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;

create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);


insert into docs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk)
    select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@docs, relative_path) as scoped_file_url,
            func.chunk as chunk
    from 
        directory(@docs),
        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, 
                              relative_path, {'mode': 'LAYOUT'})))) as func;


CREATE
OR REPLACE TEMPORARY TABLE docs_categories AS WITH unique_documents AS (
  SELECT
    DISTINCT relative_path
  FROM
    docs_chunks_table
),
docs_category_cte AS (
  SELECT
    relative_path,
    TRIM(snowflake.cortex.COMPLETE (
      'llama3-70b',
      'Given the name of the file between <file> and </file> determine if it is related to bikes or snow. Use only one word <file> ' || relative_path || '</file>'
    ), '\n') AS category
  FROM
    unique_documents
)
SELECT
  *
FROM
  docs_category_cte;

select category from docs_categories group by category;

update docs_chunks_table 
  SET category = docs_categories.category
  from docs_categories
  where  docs_chunks_table.relative_path = docs_categories.relative_path;


create or replace CORTEX SEARCH SERVICE CC_SEARCH_SERVICE_CS
ON chunk
ATTRIBUTES category
warehouse = COMPUTE_WH
TARGET_LAG = '1 minute'
as (
    select chunk,
        relative_path,
        file_url,
        category
    from docs_chunks_table
);

ALTER GIT REPOSITORY git_repo_chatbot FETCH;

-- Create stage for App logic and 3rd party packages
CREATE OR REPLACE STAGE STREAMLIT_STAGE
DIRECTORY = (ENABLE = true)
COMMENT = '{"origin": "sf_chatbot",
            "name": "chatbot",
            "version": {"major": 1, "minor": 0},
            "attributes": {"deployment": "sis"}}';

COPY FILES 
    INTO @STREAMLIT_STAGE
    FROM @CC_QUICKSTART_CORTEX_SEARCH_DOCS_TRU.PUBLIC.git_repo_chatbot/branches/main/
    FILES =('streamlit_chatbot.py', 'environment.yml');

ALTER STAGE STREAMLIT_STAGE REFRESH;

CREATE OR REPLACE STREAMLIT STREAMLIT_CHATBOT
    ROOT_LOCATION = '@STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_chatbot.py'
    TITLE = 'DOCUMENT CHATBOT'
    QUERY_WAREHOUSE = $streamlit_warehouse
    COMMENT = '{"origin": "sf_chatbot",
            "name": "chatbot",
            "version": {"major": 1, "minor": 0},
            "attributes": {"deployment": "sis"}}';  

