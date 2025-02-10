import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import numpy as np
import json

from snowflake.cortex import Complete
from trulens.apps.custom import instrument
from trulens.core import TruSession
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.providers.cortex.provider import Cortex
from trulens.core import Feedback
from trulens.core import Select
from trulens.apps.custom import TruCustomApp

#Get Snowflake session
snowpark_session = get_active_session()
snowpark_session.use_warehouse('COMPUTE_WH')
#Create database connection
tru_snowflake_connector = SnowflakeConnector(snowpark_session=snowpark_session)
tru_session = TruSession(connector=tru_snowflake_connector)


provider = Cortex(snowpark_session, "llama3.1-8b")

f_context_relevance = (
    Feedback(provider.context_relevance, name="Context Relevance")
    .on(Select.RecordCalls.retrieve.args.query)
    .on(Select.RecordCalls.retrieve.rets.collect())
    .aggregate(np.mean)
)

f_answer_relevance = (
    Feedback(provider.relevance, name="Answer Relevance")
    .on(Select.RecordCalls.retrieve.args.query)
    .on_output()
    .aggregate(np.mean)
)

#For Snowflake notebook, sentence tokenizer needs to be set to false for groundedness evaluation.
f_groundedness = (
    Feedback(lambda source, statement:
    provider.groundedness_measure_with_cot_reasons(source=source,
    statement=statement, use_sent_tokenize=False), name="Groundedness")
    .on(Select.RecordCalls.retrieve.rets[:].collect())
    .on_output()
)

pd.set_option("max_colwidth", None)

class ChatDocumentAssistant:
    def __init__(self):
        self.NUM_CHUNKS = 3  # Default number of chunks
        self.CORTEX_SEARCH_DATABASE = "CC_QUICKSTART_CORTEX_SEARCH_DOCS_TRU"
        self.CORTEX_SEARCH_SCHEMA = "PUBLIC"
        self.CORTEX_SEARCH_SERVICE = "CC_SEARCH_SERVICE_CS"
        self.COLUMNS = ["chunk", "relative_path", "category"]
        
        self.session = get_active_session()
        self.root = Root(self.session)
        self.svc = self.root.databases[self.CORTEX_SEARCH_DATABASE].schemas[self.CORTEX_SEARCH_SCHEMA].cortex_search_services[self.CORTEX_SEARCH_SERVICE]
        
        self.init_session_state()
    
    def init_session_state(self):
        if "model_name" not in st.session_state:
            st.session_state.model_name = "mistral-large2"
        if "category_value" not in st.session_state:
            st.session_state.category_value = "ALL"
        if "rag" not in st.session_state:
            st.session_state.rag = False

    def config_options(self):
        st.sidebar.selectbox('Select your model:', (
            'mistral-large2', 'llama3.1-70b',
            'llama3.1-8b', 'snowflake-arctic'
        ), key="model_name")
        
        categories = self.session.sql("select category from docs_chunks_table group by category").collect()
        cat_list = ['ALL'] + [cat.CATEGORY for cat in categories]
        st.sidebar.selectbox('Select what products you are looking for', cat_list, key="category_value")

        st.sidebar.expander("Session State").write(st.session_state)
    
    @instrument
    def retrieve(self, query):
        if st.session_state.category_value == "ALL":
            response = self.svc.search(query, self.COLUMNS, limit=self.NUM_CHUNKS)
        else:
            filter_obj = {"@eq": {"category": st.session_state.category_value}}
            response = self.svc.search(query, self.COLUMNS, filter=filter_obj, limit=self.NUM_CHUNKS)
        
        st.sidebar.json(response.results)
        return response.results
    
    def create_prompt(self, myquestion):
        if st.session_state.rag:
            prompt_context = self.retrieve(myquestion)
            
            prompt = f"""
            You are an expert chat assistant that extracts information from the CONTEXT provided
            between <context> and </context> tags. Be concise and do not hallucinate.
            If you don’t have the information, just say so. Only answer if you can extract it from the CONTEXT.
            
            <context>
            {prompt_context}
            </context>
            <question>
            {myquestion}
            </question>
            Answer:
            """
            
            relative_paths = set(item['relative_path'] for item in prompt_context)
        else:
            prompt = f"""[0]
            'Question:  {myquestion}\nAnswer: '
            """
            relative_paths = "None"
        
        return prompt, relative_paths
    
    
    @instrument
    def complete(self, myquestion):
        prompt, relative_paths = self.create_prompt(myquestion)
        df_response = Complete(st.session_state.model_name, prompt)

        return df_response, relative_paths
    
    def main(self):
        st.title(":speech_balloon: Chat Document Assistant with Snowflake Cortex")
        st.write("This is the list of documents you already have and that will be used to answer your questions:")
        
        docs_available = self.session.sql("ls @docs").collect()
        list_docs = [doc["name"] for doc in docs_available]
        st.dataframe(list_docs)
        
        self.config_options()
        st.session_state.rag = st.sidebar.checkbox('Use your own documents as context?')
        
        question = st.text_input("Enter question", placeholder="Is there any special lubricant to be used with the premium bike?", label_visibility="collapsed")
        
        if question:
            response, relative_paths = self.complete(question)
            res_text = response
            st.markdown(res_text)
            
            if relative_paths != "None":
                with st.sidebar.expander("Related Documents"):
                    for path in relative_paths:
                        cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
                        df_url_link = self.session.sql(cmd2).to_pandas()
                        url_link = df_url_link._get_value(0, 'URL_LINK')
                        display_url = f"Doc: [{path}]({url_link})"
                        st.sidebar.markdown(display_url)



if __name__ == "__main__":
    app = ChatDocumentAssistant()

    tru_app = TruCustomApp(
            app,
            app_name="ChatDocumentAssistant",
            app_version="simple",
            feedbacks=[f_groundedness, f_answer_relevance, f_context_relevance],
    )

    with tru_app as recording:  
       app.main()
