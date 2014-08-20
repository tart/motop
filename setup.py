#!/usr/bin/env python

from setuptools import setup
import libmotop

def readme():
    with open('README.rst') as readmeFile:
        return readmeFile.read()

setup(name=libmotop.__name__,
        version=str(libmotop.__version__),
        packages=('libmotop',),
        scripts=('motop',),
        install_requires=('pymongo', 'argparse'),
        author='Emre Hasegeli',
        author_email='hasegeli@tart.com.tr',
        license='ICS',
        url='https://github.com/tart/motop',
        platforms='POSIX',
        description=libmotop.__doc__,
        keywords='mongo realtime monitoring examine explain kill operations',
        long_description=readme())

