'''
Created on Mar 12, 2015

@author: pete.zybrick
'''
import os
from distutils.core import setup

try:
    os.remove('MANIFEST')
except StandardError:
    pass

setup(name='awsext',
      version='1.1',
      description='AWS Extension Classes',
      author='Pete Zybrick',
      author_email='pete.zybrick@ipc-global.com',
      url='https:// TODO:link to github',
      packages=['awsext', 'awsext.ec2', 'awsext.iam', 'awsext.s3', 'awsext.sqs', 'awsext.vpc'],
      package_dir = {'': 'src'},
     )
