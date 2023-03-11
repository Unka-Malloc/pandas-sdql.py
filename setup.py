from setuptools import setup, find_packages

setup(
    name='pd2sd',  # 在pip中显示的项目名称
    version='0.1',
    author='Yizhuo Yang',
    author_email='cxunka@outlook.com',
    url='',
    description='ts: Test Setup',
    packages=find_packages(exclude=["tests"]),
    package_data = {
        '': [
            '*.toml',
        ],
    },
    python_requires='>=3.7.0',
    install_requires=open('requirements.txt').readlines()
)
