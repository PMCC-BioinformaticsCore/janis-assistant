from setuptools import setup, find_packages

# from setuptools.command.develop import develop
# from setuptools.command.install import install

modules = ["janis_assistant." + p for p in sorted(find_packages("./janis_assistant"))]

vsn = {}
with open("./janis_assistant/__meta__.py") as fp:
    exec(fp.read(), vsn)
__version__ = vsn["__version__"]

setup(
    # legacy .runner - renamed to assistant
    name="janis-pipelines.runner",
    version=__version__,
    description="Easier way to run workflows, configurable across environments",
    long_description=open("./README.md").read(),
    long_description_content_type="text/markdown",
    author="Michael Franklin",
    author_email="michael.franklin@petermac.org",
    license="MIT",
    keywords=["janis", "workflows", "assistant"],
    entry_points={
        "console_scripts": ["janis=janis_assistant.cli:process_args"],
        "janis.extension": ["assistant=janis_assistant"],
    },
    install_requires=[
        "janis-pipelines.core>=0.11.0",
        "janis-pipelines.templates>=0.11.0",
        "requests",
        "path",
        "python-dateutil",
        "progressbar2",
        "tabulate",
        "ruamel.yaml >= 0.12.4, <= 0.16.5",
        "cwltool",
        "blessed",
    ],
    extras_require={
        "gcs": ["google-cloud-storage"],
        "ci": [
            "codecov",
            "coverage",
            "requests_mock",
            "nose_parameterized",
            "keyring==21.4.0",
            "setuptools",
            "wheel",
            "twine",
        ],
    },
    packages=["janis_assistant"] + modules,
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
    },
)
