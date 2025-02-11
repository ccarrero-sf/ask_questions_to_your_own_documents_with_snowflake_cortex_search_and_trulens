SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE or replace DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS_TRU;

CREATE OR REPLACE API INTEGRATION git_api_integration_chatbot
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/ccarrero-sf/')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY git_repo_chatbot
    api_integration = git_api_integration_chatbot
    origin = 'https://github.com/ccarrero-sf/ask_questions_to_your_own_documents_with_snowflake_cortex_search_and_trulens';

-- Make sure we get the latest files
ALTER GIT REPOSITORY git_repo_chatbot FETCH;

SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_08');
-- ENABLE LATEST PYTHON VERSIONS
SELECT SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2024_08');
-- Check it is enabled
SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_08');
