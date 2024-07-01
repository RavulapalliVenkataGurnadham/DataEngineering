import streamlit as st
# from main import main
import sys
import os 
from main import main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
summary = main()
st.write(summary)