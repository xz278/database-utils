from setuptools import setup

setup(
    name='database_utils',
    version='1.2.0',
    description='Database Utils',
    url='https://github.com/xz278/database-utils.git',
    author='Xian Zhang',
    author_email='zhang.xian.0414@outlook.com',
    packages=['database_utils'],
    zip_safe=False,
    install_requires=['requests', 'click', 'psycopg2-binary',
                      'mysql-connector-python', 'configparser', 'pandas'],
    include_package_data=False,
    # scripts=['bin/syncredash'],
    # entry_points = {
    #     'console_scripts': ['syncredash=report_redash.command_line:main'],
    # }
)
