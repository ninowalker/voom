from setuptools import setup, find_packages
from voom import __version__
import glob
import os
import sys

README = "README.md"

base = os.path.dirname(__file__)
local = lambda x: os.path.join(base, x)


def read(fname):
    try:
        return open(local(fname)).read()
    except:
        return "Not available"


def hydrate_examples():
    examples = {}
    for f in glob.glob(local('examples/*')) + glob.glob(local('tests/*')) + glob.glob(local('tests/*/*')):
        if os.path.isdir(f):
            continue
        examples[os.path.basename(f)] = "\n    ".join(read(f).split("\n"))
    #print examples.keys()
    readme = read(README + ".in") % examples
    with open(local(README), "w") as f:
        f.write(readme)

try:
    hydrate_examples()
except:
    pass


if "nosetests" in sys.argv or "test" in sys.argv:
    TEST_REQS = ['nose>=1.0', 'coverage', 'nosexcover', 'mock']
else:
    TEST_REQS = []


setup(
    name="voom",
    version=__version__,
    author="Nino Walker",
    author_email="nino.walker@gmail.com",
    description=("A python message bus that you can put 4 million volts through."),
    url='https://github.com/ninowalker/voom',
    license="BSD",
    packages=find_packages(exclude=['tests']),
    long_description=read(README),
    setup_requires=['pika', 'protobuf', 'protobuf_to_dict>=0.0.6'] + TEST_REQS,
    dependency_links=['http://github.com/Livefyre/protobuf-to-dict/tarball/master#egg=protobuf_to_dict-0.0.6'],
    test_suite='nose.collector',
    classifiers=[
        "License :: OSI Approved :: BSD License",
    ],
    #entry_points = {'console_scripts': ['']}
)
