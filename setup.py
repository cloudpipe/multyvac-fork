from setuptools import setup

# We disable requirements.txt parsing for now since users are having problems
# with their cwd being elsewhere than their requirements.txt file.
#from pip.req import parse_requirements
# parse_requirements() returns generator of pip.req.InstallRequirement objects
#install_reqs = [str(ir.req) for ir in parse_requirements('requirements.txt')]

install_reqs = ['requests>=1.1.0', 'ConcurrentLogHandler>=0.9.1']

dist = setup(
    name='vac',
    version='0.6.0',
    description='Multyvac fork for Python',
    author='Cloudpipe',
    author_email='cloudpipe@googlegroups.com',
    url='https://github.com/cloudpipe/multyvac-fork',
    install_requires=install_reqs,
    license='LICENSE.txt',
    packages=['multyvac', 'multyvac.util'],
    long_description=open('README.rst').read(),
    platforms=['CPython 2.6', 'CPython 2.7'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        ],
)
