import pathlib

from setuptools import setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="isostream",
    packages=["isostream"],
    version="1.0.1",
    description="Python Client for the IsoStream API",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/dftrading/isostream",
    author="IsoStream",
    license="MIT",
    author_email="info@isostream.io",
    install_requires=[
        "pandas",
        "dateutil",
        "requests",
    ],
)
