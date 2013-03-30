#!/usr/bin/env python

from setuptools import setup
from libmotop.Motop import Motop

def readme ():
    with open ('README') as readmeFile:
        return readmeFile.read ()

setup (name = 'motop',
        version = str (Motop.version),
        packages = ['libmotop'],
        scripts = ['motop'],
        install_requires = ['pymongo'],
        author = 'Emre Hasegeli',
        author_email = 'hasegeli@tart.com.tr',
        license = 'ICS',
        url = 'https://github.com/tart/motop',
        platforms = 'POSIX',
        description = 'Realtime monitoring tool for several MongoDB servers.',
        keywords = 'mongo realtime monitoring examine explain kill operations',
        long_description = readme ())

