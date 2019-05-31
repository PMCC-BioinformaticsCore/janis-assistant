from setuptools import setup, find_packages

__version__ = "v0.0.1"

modules = ["shepherd." + p for p in sorted(find_packages('./shepherd'))]

setup(
    name="shepherd",
    version=__version__,
    description='Easier way to run workflows, configurable across environments',
    long_description=open("./README.md").read(),
    long_description_content_type="text/markdown",
    author='Michael Franklin',
    author_email='michael.franklin@petermac.org',
    license='MIT',
    keywords=['shepherd'],
    entry_points={
        'console_scripts': ['shepherd=shepherd.cli:process_args']
    },
    install_requires=[
        'janis-pipelines[bioinformatics]>=v0.2.17',
        'requests',
        'path.py',
        'python-dotenv'
    ],
    packages=["shepherd"] + modules,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
    ],
)
