from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="MD2K: Cerebral Cortex",

    version='1.0.0',

    description='Backend data analytics platform for MD2K software',
    long_description=long_description,

    author='MD2K.org',
    author_email='dev@md2k.org',

    license='BSD2',

    classifiers=[

        'Development Status :: 3 - Alpha',

        'Intended Audience :: Healthcare Industry',
        'Intended Audience :: Science/Research',

        'License :: OSI Approved :: BSD License',

        'Natural Language :: English',

        'Programming Language :: Python :: 3',

        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: System :: Distributed Computing'
    ],

    keywords='mHealth machine-learning data-analysis',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['marshmallow',
                      'flask',
                      'numpy',
                      'scipy',
                      'sklearn',
                      'py4j',
                      'pytz',
                      'mysql-connector-python-rf',
                      'PyYAML',
                      'matplotlib',
                      'fastdtw'],


    entry_points={
        'console_scripts': [
            'main=main:main'
        ]
    }

)
