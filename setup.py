from setuptools import setup, find_packages
import os.path

# Get the long description from the relevant file
__here__ = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(__here__, 'README.md'), 'r') as f:
    long_description = f.read()

setup(
    name = 'capgains',
    version = '0.0.1dev',
    # Note: change 'master' to the tag name when release a new verison
    download_url = 'https://github.com/csingley/capgains/tarball/master',

    description = ('Library for working with Open Financial Exchange (OFX) '
                   'formatted data used by financial institutions'),
    long_description = long_description,

    url = 'https://github.com/csingley/capgains',

    author = 'Christopher Singley',
    author_email = 'csingley@gmail.com',

    license = 'MIT',

    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
        'Topic :: Office/Business',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Accounting',
        'Topic :: Office/Business :: Financial :: Investment',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords = ['tax', 'investment', 'ofx', 'Open Financial Exchange'],

    packages = find_packages(),

    install_requires = [
        'ofxtools >= 0.3.13',
        'sqlalchemy >= 1.0.0',
    ],

    package_data = {
        'capgains': ['README.md', 'tests/*'],
    },

    entry_points = {
        'console_scripts': [
            'capgains=capgains.models:main',
        ],
    },
)
