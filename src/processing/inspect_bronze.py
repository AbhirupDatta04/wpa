import pandas as pd

from src.utils.io import read_df


def inspect_dataset(layer: str, name: str):
    print(f"\n{'=' * 60}")
    print(f"{layer.upper()} → {name.upper()}")
    print(f"{'=' * 60}")

    df = read_df(layer=layer, name=name)

    print("\nShape:")
    print(df.shape)

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nSample Data:")
    print(df.head())

    print("\nData Types:")
    print(df.dtypes)


if __name__ == "__main__":
    datasets = [
        "users",
        "sessions",
        "events",
        "trades",
    ]

    for dataset in datasets:
        inspect_dataset("bronze", dataset)
