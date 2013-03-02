#!/usr/bin/env python

from setuptools import setup
from src.Motop import Motop

setup (name = 'motop',
        version = str (Motop.version),
        author = 'Emre Hasegeli',
        author_email = 'hasegeli@tart.com.tr',
        scripts = ['motop.py'],
        license = 'ICS',
        url = 'https://github.com/tart/motop',
        description = 'Realtime monitoring tool for several MongoDB servers.',
        keywords = 'mongo realtime monitoring examine explain kill operations',
        install_requires = ['pymongo'])

