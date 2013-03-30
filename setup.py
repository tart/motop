#!/usr/bin/env python

from setuptools import setup

setup (name = 'motop',
        version = '1.3',
        author = 'Emre Hasegeli',
        author_email = 'hasegeli@tart.com.tr',
        scripts = ['motop.py'],
        license = 'ICS',
        url = 'https://github.com/tart/motop',
        description = 'Realtime monitoring tool for several MongoDB servers.',
        keywords = 'mongo realtime monitoring examine explain kill operations',
        install_requires = ['pymongo'])

