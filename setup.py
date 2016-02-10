from setuptools import setup, find_packages

setup(
    name="yarn-app-sample",
    version="1.0.0",
    description="Python YARN AppMaster Sample",
    author="Kazuki Oikawa",
    author_email="k@oikw.org",
    packages=find_packages(exclude=['src'])
)
