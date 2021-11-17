from distutils.core import setup

setup(
    name="isostream",
    version="1.0",
    description="Python Client for the ISOStream API",
    author="ISOStream",
    author_email="info@isostream.io",
    url="isostream.io",
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.0",
        "requests-cache>=0.8.0",
    ],
)
