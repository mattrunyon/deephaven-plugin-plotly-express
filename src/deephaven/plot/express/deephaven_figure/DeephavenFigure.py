from __future__ import annotations

import json
import threading
from collections.abc import Generator
from typing import Callable, Any
from plotly.graph_objects import Figure

from deephaven.table_listener import TableListener
from deephaven.table import PartitionedTable
# from deephaven.plugin.object_type import Exporter

from ..data_mapping import DataMapping
from ..shared import args_copy


def export_figure(
        exporter: Exporter,
        figure: DeephavenFigure
) -> bytes:
    """Helper to export a DeephavenFigure as json

    Args:
      exporter: Exporter: The exporter to use
      figure: DeephavenFigure: The figure to export

    Returns:
      bytes: The figure as bytes

    """
    return figure.to_json(exporter).encode()


def has_color_args(
        call_args: dict[str, Any]
) -> bool:
    """Check if any of the color args are in call_args

    Args:
      call_args: dict[str, Any]: A dictionary of args

    Returns:
      bool: True if color args are in, false otherwise

    """
    for arg in ["color_discrete_sequence_line",
                "color_discrete_sequence_marker"]:
        # convert to bool to ensure empty lists don't prevent removal of
        # colors on traces
        if arg in call_args and bool(call_args[arg]):
            return True
    return False


def has_arg(
        call_args: dict[str, Any],
        check: str | Callable
) -> bool:
    """Given either a string to check for in call_args or function to check,
    return True if the arg is in the call_args

    Args:
      call_args: dict[str, Any]: A dictionary of args
      check: str | Callable: Either a string or a function that takes call_args

    Returns:
        bool: True if the call_args passes the check, False otherwise
    """
    if call_args:
        if isinstance(check, str) and check in call_args:
            return bool(call_args[check])
        elif isinstance(check, Callable):
            return check(call_args)
    return False
    # check is either a function or string


class Reference:
    """A reference to an object

    Attributes:
        index: int: The index of the reference
        obj: object: The object that the reference points to
    """

    def __init__(
            self: Reference,
            index: int,
            obj: object
    ):
        self.index = index
        self.obj = obj


class Exporter:

    def __init__(
            self: DeephavenFigureExporter,
    ):
        self.references = {}
        pass

    def reference(self: Exporter, obj: object) -> Reference:
        """Creates a reference for an object, ensuring that it is exported for
            use on the client. Each time this is called, a new reference will be
            returned, with the index of the export in the data to be sent to the
            client.

            Args:
            obj: object: The object to create a reference for

            Returns:
                Reference: The reference to the object

            """
        if obj not in self.references:
            self.references[obj] = Reference(len(self.references), obj)
        return self.references[obj]

    def reference_list(self: Exporter) -> list[Any]:
        """Creates a list of references for a list of objects

            Args:
              objs: list[object]: The list of objects to create references for

            Returns:
                list[Reference]: The list of references to the objects

            """
        return list(self.references.keys())


class DeephavenFigureListener:

    def __init__(self, table, orig_func, orig_args, exec_ctx):
        self.table = table
        self.partitions = self.partition_count()
        self.deephaven_figure = None
        self.orig_func = orig_func
        # these args should always be copied before use to prevent
        # modification
        # additionally, the compound elements themselves should not be modified
        self.orig_args = orig_args
        # the table should be the existing table
        # this will eliminate lots of the processing done as the
        # partitioned table is already created
        self.orig_args["args"]["table"] = table
        self.connection = None
        self.exporter = Exporter()
        self.exec_ctx = exec_ctx

    def partition_count(self):
        if isinstance(self.table, PartitionedTable):
            return len(self.table.constituent_tables)
        return -1

    def on_update(self, update, is_replay) -> None:
        # because this is listening to the partitioned meta table, it will
        # always trigger a rerender
        with self.exec_ctx:
            self.partitions = self.partition_count()
            new_args = args_copy(self.orig_args)
            new_fig, _ = self.orig_func(**new_args)
            new_fig.to_dict(exporter=self.exporter)

            if self.connection:
                # attempt to send
                message = {
                    "type": "NEW_FIGURE",
                    "figure": new_fig.to_dict(exporter=self.exporter)
                }

                self.connection.on_data(
                    json.dumps(message).encode(),
                    self.exporter.reference_list())

            return new_fig

    def execute(
            self: DeephavenFigureListener,
            payload: bytes,
            references: list[Any]
    ) -> tuple[bytes, list[Any]]:
        """Execute the DeephavenFigure

        Args:
          payload: bytes: The payload to execute
          references: list[Any]: The references to use

        Returns:
          tuple[bytes, list[Any]]: The result payload and references

        """
        # todo: figure out what can be received here
        return payload, references


class DeephavenFigure:
    """A DeephavenFigure that contains a plotly figure and mapping from Deephaven
    data tables to the plotly figure

    Attributes:
        fig: Figure: (Default value = None) The underlying plotly fig
        call: Callable: (Default value = None) The (usually) px drawing
          function
        call_args: dict[Any]: (Default value = None) The arguments that were
          used to call px
        _data_mappings: list[DataMapping]: (Default value = None) A list of data
          mappings from table column to corresponding plotly variable
        has_template: bool: (Default value = False) If a template is used
        has_color: bool: (Default value = False) True if color was manually
          applied via discrete_color_sequence
        trace_generator: Generator[dict[str, Any]]: (Default value = None)
          A generator for modifications to traces
        has_subplots: bool: (Default value = False) True if has subplots
    """

    def __init__(
            self: DeephavenFigure,
            fig: Figure = None,
            call: Callable = None,
            call_args: dict[Any] = None,
            data_mappings: list[DataMapping] = None,
            has_template: bool = False,
            has_color: bool = False,
            trace_generator: Generator[dict[str, Any]] = None,
            has_subplots: bool = False,
    ):
        # keep track of function that called this and it's args
        self.fig = fig
        self.call = call
        self.call_args = call_args
        self.trace_generator = trace_generator

        self.has_template = has_template if has_template else \
            has_arg(call_args, "template")

        self.has_color = has_color if has_color else \
            has_arg(call_args, has_color_args)

        self._data_mappings = data_mappings if data_mappings else []

        self.has_subplots = has_subplots

        self.listener = None

        # lock to prevent multiple threads from updating the figure at once
        self.fig_lock = threading.Lock()

    def copy_mappings(
            self: DeephavenFigure,
            offset: int = 0
    ) -> list[DataMapping]:
        """Copy all DataMappings within this figure, adding a specific offset

        Args:
          offset: int:  (Default value = 0) The offset to offset the copy by

        Returns:
          list[DataMapping]: The new DataMappings

        """
        return [mapping.copy(offset) for mapping in self._data_mappings]

    def get_json_links(
            self: DeephavenFigure,
            exporter: Exporter
    ) -> list[dict[str, str]]:
        """Convert the internal data mapping to the JSON data mapping with
        tables and proper plotly indices attached

        Args:
          exporter: Exporter: The exporter to use to send tables

        Returns:
          list[dict[str, str]]: The list of json links that map table columns
            to the plotly figure

        """
        return [links for mapping in self._data_mappings
                for links in mapping.get_links(exporter)]

    def to_dict(
            self: DeephavenFigure,
            exporter: Exporter
    ) -> dict[str, Any]:
        """Convert the DeephavenFigure to dict

        Args:
          exporter: Exporter: The exporter to use to send tables

        Returns:
          str: The DeephavenFigure as a dictionary

        """
        return json.loads(self.to_json(exporter))

    def to_json(
            self: DeephavenFigure,
            exporter: Exporter
    ) -> str:
        """Convert the DeephavenFigure to JSON

        Args:
          exporter: Exporter: The exporter to use to send tables

        Returns:
          str: The DeephavenFigure as a JSON string

        """
        plotly = json.loads(self.fig.to_json())
        mappings = self.get_json_links(exporter)
        deephaven = {
            "mappings": mappings,
            "is_user_set_template": self.has_template,
            "is_user_set_color": self.has_color
        }
        payload = {
            "plotly": plotly,
            "deephaven": deephaven
        }
        return json.dumps(payload)

    def initialize_listener(self, table, orig_func, orig_args, exec_ctx):
        self.listener = DeephavenFigureListener(table, orig_func, orig_args, exec_ctx)
        return self.on_update

    def on_update(self, update, is_replay):
        if self.listener is None:
            raise ValueError("Listener not initialized")

        with self.fig_lock:
            new_fig = self.listener.on_update(update, is_replay)

            self.fig = new_fig.fig
            self.call = new_fig.call
            self.call_args = new_fig.call_args
            self.trace_generator = new_fig.trace_generator
            self.has_template = new_fig.has_template
            self.has_color = new_fig.has_color
            self._data_mappings = new_fig._data_mappings
            self.has_subplots = new_fig.has_subplots

