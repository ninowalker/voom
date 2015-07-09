
all: test bdist_egg

test:
	python setup.py nosetests

bdist_egg:
	python setup.py bdist_egg

register:
	python setup.py register -r pypi

upload:
	python setup.py sdist upload -r pypi
