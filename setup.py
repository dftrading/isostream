from distutils.core import setup

setup(
    name="isostream",
    packages=["isostream"],
    version="1.0.1",
    description="Python Client for the ISOStream API",
    author="ISOStream",
    author_email="info@isostream.io",
    url="isostream.io",
    python_requires=">=3.6",
    install_requires=[
        "pandas",
        "dateutil",
        "requests",
    ],
)
