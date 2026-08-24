"""Labeled KPI charts for the stakeholder report.

Each KPI has a pure `_build_*_figure` function (DataFrame -> matplotlib Figure,
fully testable without touching the filesystem) and a thin `plot_*` wrapper that
saves it to disk. Uses the non-interactive Agg backend so this runs headless.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from plugins.common.config import FIGURES_DIR


def _build_avg_fare_by_airline_figure(df: pd.DataFrame):
    ordered = df.sort_values("avg_total_fare", ascending=True)

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.barh(ordered["airline"], ordered["avg_total_fare"], color="#4C72B0")
    ax.set_xlabel("Average Total Fare (BDT)")
    ax.set_ylabel("Airline")
    ax.set_title("Average Fare by Airline")
    fig.tight_layout()
    return fig


def _build_seasonal_fare_variation_figure(df: pd.DataFrame):
    ordered = df.sort_values("avg_total_fare", ascending=False)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(ordered["seasonality"], ordered["avg_total_fare"], color="#DD8452")
    ax.set_xlabel("Season")
    ax.set_ylabel("Average Total Fare (BDT)")
    ax.set_title("Seasonal Fare Variation: Peak vs. Regular")
    fig.tight_layout()
    return fig


def _build_bookings_by_airline_figure(df: pd.DataFrame):
    ordered = df.sort_values("booking_count", ascending=True)

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.barh(ordered["airline"], ordered["booking_count"], color="#55A868")
    ax.set_xlabel("Number of Bookings")
    ax.set_ylabel("Airline")
    ax.set_title("Booking Count by Airline")
    fig.tight_layout()
    return fig


def _build_top_routes_figure(df: pd.DataFrame):
    ordered = df.sort_values("rank", ascending=False)
    labels = ordered["source"] + " -> " + ordered["destination"]

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(labels, ordered["booking_count"], color="#C44E52")
    ax.set_xlabel("Number of Bookings")
    ax.set_ylabel("Route (Source -> Destination)")
    ax.set_title("Most Popular Routes (Top 10)")
    fig.tight_layout()
    return fig


def _save_figure(fig, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


def plot_avg_fare_by_airline(df: pd.DataFrame, output_path: Path | None = None) -> Path:
    output_path = Path(output_path) if output_path else FIGURES_DIR / "avg_fare_by_airline.png"
    return _save_figure(_build_avg_fare_by_airline_figure(df), output_path)


def plot_seasonal_fare_variation(df: pd.DataFrame, output_path: Path | None = None) -> Path:
    output_path = Path(output_path) if output_path else FIGURES_DIR / "seasonal_fare_variation.png"
    return _save_figure(_build_seasonal_fare_variation_figure(df), output_path)


def plot_bookings_by_airline(df: pd.DataFrame, output_path: Path | None = None) -> Path:
    output_path = Path(output_path) if output_path else FIGURES_DIR / "bookings_by_airline.png"
    return _save_figure(_build_bookings_by_airline_figure(df), output_path)


def plot_top_routes(df: pd.DataFrame, output_path: Path | None = None) -> Path:
    output_path = Path(output_path) if output_path else FIGURES_DIR / "top_routes.png"
    return _save_figure(_build_top_routes_figure(df), output_path)
