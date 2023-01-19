from setuptools import setup

description = "Async caching framework"


setup(
    name="cacheme",
    url="https://github.com/Yiling-J/cacheme",
    author="Yiling",
    author_email="njjyl723@gmail.com",
    license="BSD-3-Clause",
    version="v0.2.0",
    packages=["cacheme", "cacheme.storages"],
    description=description,
    python_requires=">=3.7",
    install_requires=[
        "msgpack",
        "pydantic",
        "xxhash",
        "typing_extensions",
        "cacheme_utils",
    ],
    zip_safe=False,
    extras_require={
        "postgresql": ["asyncpg"],
        "mysql": ["aiomysql"],
        "redis": ["redis"],
        "mongo": ["motor"],
    },
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
