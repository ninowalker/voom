
all: test bdist_egg

test:
	python setup.py nosetests

bdist_egg:
	python setup.py bdist_egg