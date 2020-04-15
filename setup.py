from setuptools import setup

description = 'A memoized/cache decorator for Python using redis.'


setup(
    name="cacheme",
    url="https://github.com/Yiling-J/cacheme",
    author="Yiling",
    author_email="njjyl723@gmail.com",
    license="BSD-3-Clause",
    version='v0.1.1',
    packages=[
        "cacheme",
    ],
    description=description,
    python_requires=">=3.5",
    install_requires=[
        "redis>=3.0.0",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
)
