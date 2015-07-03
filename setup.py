"""A distributed execution engine.
See:
https://github.com/gtoonstra/remap
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='remap',
    version='0.0.1',
    description='Distributed execution engine',
    long_description=long_description,
    url='https://github.com/gtoonstra/remap',
    author='Gerard Toonstra',
    author_email='gtoonstra@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: C',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    keywords='pregel mapreduce grid',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    include_package_data=True,
    zip_safe=False,
    scripts=['remap/bin/remap'],
    install_requires=['nanomsg','flask','flask-simple-api'],
    extras_require={},
    download_url=(
        'https://github.com/gtoonstra/remap/tarball/' + version),
)
