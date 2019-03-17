from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='pyNakadi',
      version='0.2.13',
      description='Python client for Nakadi',
      long_description=readme(),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Programming Language :: Python :: 3.5',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Software Development :: Libraries :: Python Modules'
      ],
      keywords='python client nakadi reader',
      url='https://github.com/eiunkar/pyNakadi',
      author='Ersin Ihsan Unkar',
      author_email='eiunkar@gmail.com',
      packages=['pyNakadi'],
      install_requires=[
          'requests'
      ],
      test_suite='nose.collector',
      tests_require=[],
      include_package_data=True,
      zip_safe=False)
