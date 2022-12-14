from setuptools import setup
from Cython.Build import cythonize

setup(
    name='tbl transformer',
    ext_modules = cythonize("tbl_to_csv_transformer.pyx"),
    zip_safe=False,
)