#!/usr/bin/env python

from setuptools import setup
from libmotop import motop

def readme():
    with open('README.rst') as readmeFile:
        return readmeFile.read()

setup(name=motop.__name__,
        version=str(motop.__version__),
        packages=('libmotop',),
        scripts=('motop',),
        install_requires=('pymongo', 'argparse'),
        author='Emre Hasegeli',
        author_email='hasegeli@tart.com.tr',
        license='ICS',
        url='https://github.com/tart/motop',
        platforms='POSIX',
        description=motop.__doc__,
        keywords='mongo realtime monitoring examine explain kill operations',
        long_description=readme())

