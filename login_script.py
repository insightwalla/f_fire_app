from typing import Callable
import yaml
from yaml import SafeLoader
from streamlit_authenticator import Authenticate
import streamlit as st
from utils import *


def login(render_func: Callable[[str], None], config_file: str, section: str = 'Product'):
    with open(config_file) as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )

    c1, c2 = st.columns(2)
    with c1:
        name, authentication_status, username = authenticator.login('Login', 'main')

    if authentication_status: 
        with c1:
            authenticator.logout('Logout', 'main')

        c2.write(f'Welcome *{name}*')
        # call the rendering function
        render_func(name_user = name)

    else: # no login attempt
        c1.warning('Please enter your username and password')

'''
# testing
config_file = "config.yaml"
login(render_func=function_main, 
      config_file=config_file, 
      section='Product')
'''