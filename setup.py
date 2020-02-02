from setuptools import setup
from readme_renderer import markdown


def readme():
    with open('README.md') as f:
        return f.read()


def get_version():
    with open('VERSION') as f:
        return f.read()


setup(name='pyNakadi',
      version=get_version(),
      description='Python client for Nakadi',
      long_description=markdown.render(readme()),
      long_description_content_type="text/markdown",
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Programming Language :: Python :: 3.6',
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
