import os
import pathlib
from setuptools import setup, find_packages
from pathlib import Path


def _get_files(path: pathlib.Path, suffix: str) -> list[str]:
    ret: list[str] = []
    if path.is_dir():
        for p in path.iterdir():
            ret.extend(_get_files(p, suffix))
    elif path.suffix == suffix:
        ret.append(str(path))
    return ret


package_name = "docdblite"
root_dir = Path(__file__).parent.absolute()

package_dir = root_dir / "docdblite"
pyi_files = [
    p.replace(str(package_dir) + os.sep, "").replace("\\", "/")
    for p in _get_files(package_dir, ".pyi")
]

setup(
    name=package_name,
    version="0.0.1",
    description="NoSQL on SQL",
    long_description=(root_dir / "README.md").open("r", encoding="utf8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kniwase/docdblite",
    author="Kento Niwase",
    author_email="kento.niwase@outlook.com",
    license="MIT",
    keywords="browser,frontend,framework,front-end,client-side",
    packages=find_packages(),
    package_data={
        "docdblite": [
            "py.typed",
            *pyi_files,
        ],
    },
    install_requires=[
        name.rstrip()
        for name in (root_dir / "requirements.txt")
        .open("r", encoding="utf8")
        .readlines()
    ],
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
