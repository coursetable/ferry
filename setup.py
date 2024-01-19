from setuptools import find_packages, setup

setup(
    name='ferry',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'requests==2.28.1',
        'beautifulsoup4==4.11.1',
        'stackprinter==0.2.10',
        'sentry-sdk==1.12.1',
        'tqdm==4.64.1',
        'unidecode==1.3.6',
        'tika==1.25',
        'regex==2022.10.31',
        'pandas==1.5.2',
        'vadersentiment==3.3.2',
        'ujson==4.3.0',
        'textdistance==4.5.0',
        'sqlalchemy-mixins==1.5.3',
        'sqlalchemy==1.4.45',
        'uvloop==0.19.0',
        'httpx==0.26.0',
        "PyYAML==6.0.1",
        "lxml==5.0.0",
        "diskcache==5.6.3"
    ],
)