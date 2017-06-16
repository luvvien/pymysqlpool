from distutils.core import setup

setup(
    name='pymysqlpool',
    version='0.1',
    packages=['pymysqlpool'],
    url='',
    license='MIT',
    author='Christopher Lee',
    author_email='',
    requires=['pymysql', 'pandas'],
    description='MySQL connection pool utility.'
)
