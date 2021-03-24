from distutils.core import setup

with open("README.md", "r") as fd:
    long_describtion = fd.read()

setup(
    name = "pipeline_cluster",
    version = "1.0",
    author = "Kai Leßmeister",
    author_email = "lessmeister.kai@gmail.com",
    description = "A simple pipeline cluster implementation in python",
    long_description = long_describtion,
    license = "MIT",
    keywords = "pipeline cluster parallel concurrent computing",
    packages = ["pipeline_cluster"],
    scripts=["scripts/pcluster"],
    install_requires = []
)