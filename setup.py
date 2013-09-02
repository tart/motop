#!/usr/bin/env python

from setuptools import setup
from libmotop.Motop import Motop

def readme ():
    with open ('README.md') as readmeFile:
        return readmeFile.read ()

setup (name = 'motop',
        version = str (Motop.version),
        packages = ['libmotop'],
        scripts = ['motop'],
        install_requires = ['pymongo', 'argparse'],
        author = 'Emre Hasegeli',
        author_email = 'hasegeli@tart.com.tr',
        license = 'ICS',
        url = 'https://github.com/tart/motop',
        platforms = 'POSIX',
        description = '"Top" clone for MongoDB.',
        keywords = 'mongo realtime monitoring examine explain kill operations',
        long_description = readme ())

