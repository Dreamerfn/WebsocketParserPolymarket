from setuptools import find_packages, setup

setup(
    name="websocket-parser",
    version="0.1.0",
    description="24/7 Polymarket + Binance order book collector with 12ms snapshots to Parquet",
    python_requires=">=3.9",
    packages=find_packages(include=["collector", "collector.*"]),
    install_requires=[
        "aiohttp>=3.10.0",
        "websockets>=12.0",
        "pyarrow>=16.0.0",
        "PyYAML>=6.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ws-collector=collector.__main__:main",
        ]
    },
)
