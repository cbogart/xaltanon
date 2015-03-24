from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()


version = '0.1'

install_requires = [
    "MySQL",
]


setup(name='xaltanon',
    version=version,
    description="Anonymize exported XALT database and prepare data for publication",
    long_description=README,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='hpc database xalt',
    author='Christopher Bogart',
    author_email='cbogart@cs.cmu.edu',
    url='',
    license='Apache 2.0',
    packages=find_packages(),
    zip_safe=False,
    install_requires=install_requires,
    entry_points={
        'console_scripts':
            ['xaltanon=xaltanon:main']
    }
)
