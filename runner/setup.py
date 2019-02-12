from setuptools import setup

__version__ = "v0.0.5"

setup(name="sherpherd-pipelines",
      version=__version__,
      description='Easier way to run workflows, configurable across environments',
      long_description=open("../README.md").read(),
      long_description_content_type="text/markdown",
      author='Michael Franklin',
      author_email='michael.franklin@petermac.org',
      license='MIT',
      keywords=['shepherd'],
      install_requires=[],
      packages=["runner", "engines", "engines.cromwell"],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Topic :: Scientific/Engineering',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'Environment :: Console',
      ],
)
