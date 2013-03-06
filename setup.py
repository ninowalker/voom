import os
from setuptools import setup

from celerybus import __version__

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "celerybus",
    version = __version__,
    author = "Nino Walker",
    author_email = "nino.walker@gmail.com",
    description = ("A synchronous/asynchronous message bus implementation on top of the celery task distribution framework."),
    url='https://github.com/Livefyre/celerybus',
    license = "BSD",
    packages=['celerybus'],
    long_description=read('README'),
    setup_requires=['nose>=1.0', 'coverage', 'nosexcover', 'mock'],
    test_requires=['mock', 'redis', 'requests'],
    test_suite = 'nose.collector',
    classifiers=[
        "License :: OSI Approved :: BSD License",
    ],
    #entry_points = {'console_scripts': ['']}
)
