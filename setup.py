from setuptools import setup, find_packages
# from setuptools.command.develop import develop
# from setuptools.command.install import install

modules = ["janis_runner." + p for p in sorted(find_packages("./janis_runner"))]

vsn = {}
with open("./janis_runner/__meta__.py") as fp:
    exec(fp.read(), vsn)
__version__ = vsn["__version__"]

setup(
    name="janis-pipelines.runner",
    version=__version__,
    description="Easier way to run workflows, configurable across environments",
    long_description=open("./README.md").read(),
    long_description_content_type="text/markdown",
    author="Michael Franklin",
    author_email="michael.franklin@petermac.org",
    license="MIT",
    keywords=["janis", "workflows", "enginerunner"],
    entry_points={
        "console_scripts": ["janis=janis_runner.cli:process_args"],
        "janis.extension": ["runner=janis_runner"]
    },
    install_requires=[
        "janis-pipelines.core>=0.5.0",
        "requests",
        "path.py",
        "python-dotenv",
        "python-dateutil",
    ],
    packages=["janis_runner"] + modules,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Environment :: Console",
    ],
    cmdclass={
        # 'develop': PostDevelopCommand,
        # 'install': PostInstallCommand
    }
)
