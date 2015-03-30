# Import flask and template operators
from flask import Flask

# Define the WSGI application object
app = Flask(__name__)

# Configurations
app.config.from_object('config')

from app import web
