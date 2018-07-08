from setuptools import setup, find_packages

readme = open('README.rst').read()
history = open('HISTORY.rst').read()
reqs = [x.strip() for x in open('requirements.txt').readlines()]
test_reqs = [x.strip() for x in open('requirements-tests.txt').readlines()]

setup(
    name='xrpc',
    version='0.0.1a0',
    author='Andrey Cizov',
    author_email='acizov@gmail.com',
    packages=find_packages(include=('xrpc', 'xrpc.*',)),
    description='Python 3 type hinted protobuf binding generator',
    keywords='',
    url='https://github.com/andreycizov/python-xrpc',
    include_package_data=True,
    long_description=readme,
    install_requires=reqs,
    test_suite='tests',
    tests_require=test_reqs,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
    ]
)
