import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "celerybus",
    version = "0.0.1",
    author = "Nino Walker",
    author_email = "nino.walker@gmail.com",
    description = ("A message bus implementation on top of the celery task distribution framework."),
    url='https://github.com/Livefyre/celerybus',
    license = "BSD",
    packages=['celerybus'],
    long_description=read('README'),
    install_requires=['celery>=2.5.1',],
    test_suite="tests",
    classifiers=[
        "License :: OSI Approved :: BSD License",
    ],
    #entry_points = {'console_scripts': ['']}
)