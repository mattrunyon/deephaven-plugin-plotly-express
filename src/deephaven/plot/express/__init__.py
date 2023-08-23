from __future__ import annotations

from typing import Any

from deephaven.plugin import Registration, Callback
from deephaven.plugin.object_type import Exporter, FetchOnlyObjectType, BidirectionalObjectType, MessageStream

from .deephaven_figure import DeephavenFigure, export_figure
from .deephaven_figure.DeephavenFigure import DeephavenFigureListener

from .plots import (
    area,
    bar,
    frequency_bar,
    timeline,
    histogram,
    box,
    violin,
    strip,
    ohlc,
    candlestick,
    treemap,
    sunburst,
    icicle,
    funnel,
    funnel_area,
    line,
    line_polar,
    line_ternary,
    line_3d,
    scatter,
    scatter_3d,
    scatter_polar,
    scatter_ternary,
    pie,
    layer,
    make_subplots,
)

from .data import data_generators

__version__ = "0.0.7.dev0"

NAME = "deephaven.plot.express.DeephavenFigure"


class DeephavenFigureConnection(MessageStream):
    def __init__(self, figure_listener: DeephavenFigure, client_connection: MessageStream):
        super().__init__()
        self.figure_listener = figure_listener
        self.client_connection = client_connection
        figure_listener.connection = client_connection

    def on_data(self, payload: bytes, references: list[Any]) -> None:
        """
        Args:
            payload: Payload to execute
            references: References to objects on the server

        Returns:
            None
        """
        result_payload, result_references = self.figure_listener.execute(payload, references)
        self.client_connection.on_data(result_payload, result_references)

    def on_close(self):
        pass


class DeephavenFigureType(FetchOnlyObjectType):
    """
    DeephavenFigureType for plugin registration

    """

    @property
    def name(self) -> str:
        """
        Returns the name of the plugin

        Returns:
            str: The name of the plugin

        """
        return NAME

    def is_type(self, obj: any) -> bool:
        """
        Check if an object is a DeephavenFigure

        Args:
          obj: any: The object to check

        Returns:
            bool: True if the object is of the correct type, False otherwise
        """
        return isinstance(obj, DeephavenFigure)

    def to_bytes(self, exporter: Exporter, figure: DeephavenFigure) -> bytes:
        """
        Converts a DeephavenFigure to bytes

        Args:
          exporter: Exporter: The exporter to use
          figure: DeephavenFigure: The figure to convert

        Returns:
            bytes: The Figure as bytes
        """
        return export_figure(exporter, figure)



class DeephavenFigureListenerType(BidirectionalObjectType):
    """
    DeephavenFigureType for plugin registration

    """

    @property
    def name(self) -> str:
        """
        Returns the name of the plugin

        Returns:
            str: The name of the plugin

        """
        return NAME + "New"

    def is_type(self, obj: any) -> bool:
        """
        Check if an object is a DeephavenFigure

        Args:
          obj: any: The object to check

        Returns:
            bool: True if the object is of the correct type, False otherwise
        """
        return isinstance(obj, DeephavenFigure)

    def create_client_connection(self, obj: DeephavenFigure, connection: MessageStream) -> MessageStream:
        """
        Create a client connection for the DeephavenFigure

        Args:
          obj: object: The object to create the connection for
          connection: MessageStream: The connection to use

        Returns:
            MessageStream: The client connection
        """
        print("Creating client connection", obj, connection)
        return DeephavenFigureConnection(obj, connection)


class ChartRegistration(Registration):
    """
    Register the DeephavenFigureType

    """

    @classmethod
    def register_into(cls, callback: Callback) -> None:
        """
        Register the DeephavenFigureType

        Args:
          callback: Registration.Callback:
            A function to call after registration

        """
        callback.register(DeephavenFigureType)
        callback.register(DeephavenFigureListenerType)
