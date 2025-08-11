''' setup.py
'''
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='cnparser',
    version='1.7.0',
    author='new-village',
    url='https://github.com/new-village/cnparser',
    description='cnparser is a parser library of Corporate Number Publication Site data.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license = 'Apache-2.0 license',
    install_requires=['requests', 'bs4', 'pandas', 'pandarallel', 'pykakasi'],
    packages=find_packages(),
    include_package_data=True,
    package_data={'cnparser': ['config/*.json']},
)
