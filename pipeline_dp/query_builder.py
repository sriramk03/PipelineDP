# Copyright 2025 OpenMined.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Advanced query builder API."""

import abc
import dataclasses
from typing import Any, List, Optional, Sequence

from pipeline_dp import dp_engine as dpeng
from pipeline_dp import aggregate_params
from pipeline_dp import budget_accounting as ba
from pipeline_dp import pipeline_backend


@dataclasses.dataclass
class ColumnNames:
    """Specifies column names."""
    names: List[str]

    def __init__(self, *names: str):
        self.names = list(names)


@dataclasses.dataclass
class ValuesList:
    """Specifies a list of values."""
    values: Optional[List[Any]]

    def __init__(self, *values: Any):
        self.values = list(values)


class ContributionBoundingLevel(abc.ABC):
    """Abstract class for contribution bounding level."""
    pass


@dataclasses.dataclass
class PartitionLevel(ContributionBoundingLevel):
    """Specifies partition level contribution bounding."""
    max_partitions_contributed: int
    max_contributions_per_partition: int


@dataclasses.dataclass
class RecordLevel(ContributionBoundingLevel):
    """Specifies record level contribution bounding."""
    max_partitions_contributed: int
    # max_contributions_per_partition is implicitly 1


@dataclasses.dataclass
class Budget:
    """Specifies the budget for a DP aggregation."""
    epsilon: float
    delta: float = 0.0


class GroupBySpec(abc.ABC):
    """Abstract class for specifying grouping."""
    pass


@dataclasses.dataclass
class OptimalGroupSelectionGroupBySpec(GroupBySpec):
    """Specifies optimal group selection."""
    privacy_unit: Optional[ColumnNames]
    default_values_to_ignore: Optional[ValuesList]
    budget: Optional[Budget]
    contribution_bounding_level: ContributionBoundingLevel
    min_privacy_units_per_group: Optional[int] = None
    public_groups: Optional[Sequence[Any]] = None


class CountSpec(abc.ABC):
    pass


@dataclasses.dataclass
class CountSpec(CountSpec):
    """Specifies parameters for a COUNT aggregation."""
    budget: Budget
    privacy_unit: Optional[ColumnNames] = None
    default_values_to_ignore: Optional[ValuesList] = None
    contribution_bounding_level: Optional[ContributionBoundingLevel] = None


class Query:
    """Represents a DP query."""

    def __init__(self, data, group_by_key: ColumnNames,
                 group_by_spec: GroupBySpec, aggregations):
        self._data = data
        self._group_by_key = group_by_key
        self._group_by_spec = group_by_spec
        self._aggregations = aggregations

    def run(self, test_mode: bool = False):
        """Runs the DP query."""
        # The implementation will be added in a subsequent PR.
        raise NotImplementedError("The query execution is not yet implemented in this skeleton API.")


class AggregationBuilder:
    def __init__(self, data, group_by_key, group_by_spec):
        self._data = data
        self._group_by_key = group_by_key
        self._group_by_spec = group_by_spec
        self._aggregations = []

    def count(self, output_column_name: str, spec: CountSpec) -> 'AggregationBuilder':
        """Schedules the count aggregation."""
        self._aggregations.append(("count", output_column_name, spec, {}))
        return self

    def build(self) -> Query:
        """Builds the query."""
        return Query(self._data, self._group_by_key, self._group_by_spec,
                     self._aggregations)


class GroupByBuilder:
    def __init__(self, data):
        self._data = data

    def group_by(self, group_keys: ColumnNames,
                 spec: GroupBySpec) -> 'AggregationBuilder':
        """Specifies how to group the data."""
        return AggregationBuilder(self._data, group_keys, spec)


class QueryBuilder:
    """Builds DP queries."""

    def __init__(self):
        self._data = None

    def from_(self, data, *args) -> 'GroupByBuilder':
        """Specifies the data to be processed."""
        self._data = data
        return GroupByBuilder(data)
