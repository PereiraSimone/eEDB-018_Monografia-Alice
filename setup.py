# setup.py

from setuptools import setup, find_packages

setup(
    name='hal9000_dq_engine',
    version='1.0.0',
    packages=find_packages(),
    description='Framework Dinâmico de Qualidade de Dados para Big Data.',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    author='Simone Pereira Pinto',
    email='simone.pereira.pinto@usp.br',  
    url='github.com/simonepp/analyses-data-quality-framework/hal9000_dq_engine',
    install_requires=[
        'pyspark==3.4.0',  
        # - 'jinja2>=3.0.0', # Removido, não geramos mais HTML
        'requests>=2.25.1', 
        'numpy>=1.21.0',
        'pyarrow>=10.0.0' # Adicionado na etapa anterior, essencial para Parquet
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Operating System :: OS Independent',
        'Topic :: Scientific/Engineering :: Data Analysis',
    ],
    package_dir={'': '.'},
    include_package_data=True,
    python_requires='>=3.8',
    zip_safe=False,
)