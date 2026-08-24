import pandas as pd

from plugins.tasks.report_plots import (
    _build_avg_fare_by_airline_figure,
    _build_bookings_by_airline_figure,
    _build_seasonal_fare_variation_figure,
    _build_top_routes_figure,
    plot_avg_fare_by_airline,
    plot_bookings_by_airline,
    plot_seasonal_fare_variation,
    plot_top_routes,
)


def test_avg_fare_by_airline_figure_has_labeled_axes_and_title():
    df = pd.DataFrame({"airline": ["A", "B"], "avg_total_fare": [100.0, 200.0]})

    fig = _build_avg_fare_by_airline_figure(df)
    ax = fig.axes[0]

    assert ax.get_xlabel() == "Average Total Fare (BDT)"
    assert ax.get_ylabel() == "Airline"
    assert ax.get_title() == "Average Fare by Airline"


def test_seasonal_fare_variation_figure_has_labeled_axes_and_title():
    df = pd.DataFrame(
        {
            "seasonality": ["Regular", "Eid"],
            "avg_total_fare": [100.0, 150.0],
            "booking_count": [10, 5],
        }
    )

    fig = _build_seasonal_fare_variation_figure(df)
    ax = fig.axes[0]

    assert ax.get_xlabel() == "Season"
    assert ax.get_ylabel() == "Average Total Fare (BDT)"
    assert ax.get_title() == "Seasonal Fare Variation: Peak vs. Regular"


def test_bookings_by_airline_figure_has_labeled_axes_and_title():
    df = pd.DataFrame({"airline": ["A", "B"], "booking_count": [10, 20]})

    fig = _build_bookings_by_airline_figure(df)
    ax = fig.axes[0]

    assert ax.get_xlabel() == "Number of Bookings"
    assert ax.get_ylabel() == "Airline"
    assert ax.get_title() == "Booking Count by Airline"


def test_top_routes_figure_has_labeled_axes_and_title():
    df = pd.DataFrame(
        {
            "source": ["DAC", "CXB"],
            "destination": ["CXB", "DAC"],
            "booking_count": [10, 5],
            "rank": [1, 2],
        }
    )

    fig = _build_top_routes_figure(df)
    ax = fig.axes[0]

    assert ax.get_xlabel() == "Number of Bookings"
    assert ax.get_ylabel() == "Route (Source -> Destination)"
    assert ax.get_title() == "Most Popular Routes (Top 10)"


def test_plot_functions_save_a_nonempty_file(tmp_path):
    avg_fare_df = pd.DataFrame({"airline": ["A", "B"], "avg_total_fare": [100.0, 200.0]})
    seasonal_df = pd.DataFrame(
        {
            "seasonality": ["Regular", "Eid"],
            "avg_total_fare": [100.0, 150.0],
            "booking_count": [10, 5],
        }
    )
    bookings_df = pd.DataFrame({"airline": ["A", "B"], "booking_count": [10, 20]})
    routes_df = pd.DataFrame(
        {
            "source": ["DAC", "CXB"],
            "destination": ["CXB", "DAC"],
            "booking_count": [10, 5],
            "rank": [1, 2],
        }
    )

    paths = [
        plot_avg_fare_by_airline(avg_fare_df, tmp_path / "avg_fare.png"),
        plot_seasonal_fare_variation(seasonal_df, tmp_path / "seasonal.png"),
        plot_bookings_by_airline(bookings_df, tmp_path / "bookings.png"),
        plot_top_routes(routes_df, tmp_path / "routes.png"),
    ]

    for path in paths:
        assert path.exists()
        assert path.stat().st_size > 0
