from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

requires = [
    'click',
    'python-dotenv==0.21.0',
    'pyspark==3.3.1',
    'PyArrow==10.0.1',
    'koalas==1.8.2',
    'pandera[pyspark]==0.13.4',
    'sf-hamilton==1.14.1',
    'pandas',
    'numpy==1.23.1',
    'graphviz'

]

testing = ['nose2', 'parameterized']

linting_requires = [
    'flake8',
    'isort==5.10.1',
]

advanced_linting_requires = [
    'flake8',
    'flake8-docstrings==1.6.0',
    'pylint==2.4.4',
]

setup(
    name='MedTest proj',
    version='0.0.1',
    install_requires=requires,
    extras_require={
        'linting': linting_requires,
        'testing': testing,
        'advanced_linting': advanced_linting_requires,
    },
    # TODO add description
    description='',
    long_description=long_description,
    long_description_content_type="text/markdown",
    # TODO add github url
    url='',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    entry_points={'console_scripts': [
        'main=main:main',
    ]}
)
