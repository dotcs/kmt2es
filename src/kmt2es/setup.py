from setuptools import setup

setup(name='kmt2es',
      version='1.0.0',
      description='Import tours that have been recorded by Komoot into an ElasticSearch database',
      url='',
      author='Fabian Mueller',
      author_email='git@dotcs.me',
      license='CC BY-SA 4.0',
      packages=['kmt2es'],
      install_requires=[
          'argparse',
          'elasticsearch',
          'requests',
          'iso8601',
          'mpu'
      ],
      zip_safe=False)
