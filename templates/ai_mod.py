'''https://openai.com/enterprise-privacy'''
# brain emojis -> 🧠
import openai
import langchain
import streamlit as st
langchain.verbose = False
openai.api_key = st.secrets["openai_api_key"]
from utils import *

def final_page_ai(data, name_user = 'User'):
    from dotenv import load_dotenv
    from langchain.text_splitter import CharacterTextSplitter
    from langchain.embeddings import OpenAIEmbeddings, HuggingFaceInstructEmbeddings
    from langchain.vectorstores import FAISS
    from langchain.chat_models import ChatOpenAI
    from langchain.memory import ConversationBufferMemory
    from langchain.chains import ConversationalRetrievalChain
    from langchain.llms import HuggingFaceHub
    
    def get_text(data):
        data = data[data['Details'] != '']
        data = data[data['Details'] != 'nan']
        values_list = []
        for c in ['Details', 'Reservation_Venue']:
            values = data[c].tolist()
            # transform in strings
            values = [f'{c}: {i}' for i in values]
            values_list.append(values)
        return ' '.join([' '.join(i) for i in zip(*values_list)])

    def get_text_chunks(text):
        text_splitter = CharacterTextSplitter(
            separator="\n",
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len
        )
        chunks = text_splitter.split_text(text)
        return chunks

    def get_vectorstore(text_chunks):
        embeddings = OpenAIEmbeddings(openai_api_key = st.secrets["openai_api_key"])
        # embeddings = HuggingFaceInstructEmbeddings(model_name="hkunlp/instructor-xl")
        vectorstore = FAISS.from_texts(texts=text_chunks, embedding=embeddings)
        return vectorstore

    def get_conversation_chain(vectorstore):
        llm = ChatOpenAI(openai_api_key= st.secrets["openai_api_key"])
        # llm = HuggingFaceHub(repo_id="google/flan-t5-xxl", model_kwargs={"temperature":0.5, "max_length":512})
        memory = ConversationBufferMemory(
            memory_key='chat_history', return_messages=True)
        conversation_chain = ConversationalRetrievalChain.from_llm(
            llm=llm,
            retriever=vectorstore.as_retriever(),
            memory=memory
        )
        return conversation_chain

    def handle_userinput(user_question):
            try:
                #st.write('trying')
                response = st.session_state.conversation({'question': user_question})
                st.session_state.chat_history = response['chat_history']
            except:
                #st.write('except')
                st.session_state.raw_text = get_text(data)
                st.session_state.text_chunks = get_text_chunks(st.session_state.raw_text)
                vectorstore = get_vectorstore(st.session_state.text_chunks)
                st.session_state.conversation = get_conversation_chain(
                    st.session_state.vectorstore)
                response = st.session_state.conversation({'question': user_question})
                st.session_state.chat_history = response['chat_history']

    def render_chat_history():
        # reverse the order of the chat history
        messages = st.session_state.chat_history
        if messages:
            for i, message in enumerate(messages):
                c1,c2 = st.columns(2)
                if i % 2 == 0:
                    with st.chat_message(name = name_user, avatar = 'user'):
                        st.write(message.content)
                else:   
                    with st.chat_message(name = 'AI', avatar = 'assistant'):
                        st.write(message.content)

    def main():
        load_dotenv()
        if "conversation" not in st.session_state:
            st.session_state.conversation = None
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = None
        if "raw_text" not in st.session_state:
            st.session_state.raw_text =  get_text(data)
        if "text_chunks" not in st.session_state:
            st.session_state.text_chunks = get_text_chunks(st.session_state.raw_text)
        if "vectorstore" not in st.session_state:
            st.session_state.vectorstore = get_vectorstore(st.session_state.text_chunks)


        question_1 = 'Generate a in depth report for each restaurant - Highlight positive and negative points (make a numbered list) and talk about ways to improve.'
        question_2 = 'Find the worst reviews - render a numbered list of the worst reviews'
        question_3 = 'Find the best reviews - render a numbered list of the best reviews'

        def ask_this(question):
            handle_userinput(question)

        button_question_1 = st.sidebar.button('Generate Report', use_container_width= True, on_click=ask_this, args=(question_1,))
        button_question_2 = st.sidebar.button('Find the worst reviews', use_container_width= True, on_click=ask_this, args=(question_2,))
        button_question_3 = st.sidebar.button('Find the best reviews', use_container_width= True, on_click=ask_this, args=(question_3,))
        
        user_question = st.chat_input("Ask a question about the reviews:")
        if user_question:
            handle_userinput(user_question)

        # handle reset
        if st.button("New Chat", use_container_width= True, type="primary"):
            st.session_state.conversation = None
            st.session_state.chat_history = None

        render_chat_history()

    main()