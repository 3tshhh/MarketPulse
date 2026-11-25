"""Process-wide singletons + sidebar settings shared by all pages."""
import streamlit as st

from lib.hive import Hive
from lib.live import LiveState


@st.cache_resource
def live():
    return LiveState()


@st.cache_resource
def hive():
    return Hive()


def tz():
    return st.session_state.get("tz", "UTC")
