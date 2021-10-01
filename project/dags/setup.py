from pygments.lexer import include
from setuptools import setup, find_packages

setup(
    name='dwhutils',
    version='0.2.0',
    description='get utils for bitemporal data warehousing with mariadb',
    author='PoeticDrunkenCat',
    author_email='poeticdrunkencat@gmail.com',
    install_requires=["python-dotenv",],
    packages=find_packages(include=['dwhutils']),
    include_package_data=True,
)