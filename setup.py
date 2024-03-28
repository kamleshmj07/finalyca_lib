from setuptools import setup, find_packages

setup(
    name="finalyca_lib",
    version="0.1",
    description="Shared python functions for Finalyca",
    long_description="Has functions about database access, models, business logics, ORM engines etc.",
    author="Finalyca",
    packages=find_packages(include=['admin_store', 'async_tasks', 'analytics', 'data', 'bizlogic', 'cas_parser', 'fin_resource', 'fin_models', 'utils', 'sebi_lib']),
    install_requires=[
        'Flask==2.0.2',
        'SQLAlchemy==1.4.29',
        'PyYAML==6.0',
        'validators==0.18.2',
        'SQLAlchemy-Utils==0.38.2',
        'requests==2.27.1',
        'pdfplumber==0.7.4'
    ],
    include_package_data=True,
    package_data={
        # If any package contains *.txt files, include them:
        "": ["*.txt"],
        "": ["*.html"],
        "": ["*.css"],
        "": ["*.ttf"],
        "": ["*.png"],
        "": ["*.svg"],
    }
)
