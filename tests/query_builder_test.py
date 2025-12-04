import unittest
from pipeline_dp import query_builder as qb


class AdvancedQueryBuilderTest(unittest.TestCase):
    def test_query_construction(self):
        # Input data
        data = [
            {'movie_id': 'm1', 'user_id': 'u1', 'rating': 5},
            {'movie_id': 'm1', 'user_id': 'u2', 'rating': 4},
            {'movie_id': 'm1', 'user_id': 'u3', 'rating': 3},
            {'movie_id': 'm2', 'user_id': 'u1', 'rating': 2},
            {'movie_id': 'm2', 'user_id': 'u4', 'rating': 1},
        ]

        # 1. Define Query using the new direct dataclass instantiation
        query = (
            qb.QueryBuilder()
            .from_(data)
            .group_by(
                qb.ColumnNames("movie_id"),
                # Use direct constructor, not the old .Builder()
                qb.OptimalGroupSelectionGroupBySpec(
                    privacy_unit=qb.ColumnNames("user_id"),
                    contribution_bounding_level=qb.PartitionLevel(
                        max_partitions_contributed=3,
                        max_contributions_per_partition=1
                    ),
                    budget=qb.Budget(epsilon=1.0),
                    public_groups=["m1", "m2"],
                    default_values_to_ignore=None,
                )
            )
            .count(
                "rating_count",
                # Use direct constructor for CountSpec
                qb.CountSpec(
                    budget=qb.Budget(epsilon=1.0),
                    privacy_unit=qb.ColumnNames("user_id"),
                    contribution_bounding_level=qb.PartitionLevel(
                        max_partitions_contributed=3,
                        max_contributions_per_partition=1
                    )
                )
            )
            .build()
        )

        # 2. Verify that the Query object is constructed correctly (optional but good practice)
        self.assertIsNotNone(query)
        self.assertEqual(len(query._aggregations), 1)
        self.assertIsInstance(query._aggregations[0][2], qb.CountSpec)

        # 3. Verify that run() raises NotImplementedError
        with self.assertRaises(NotImplementedError):
            query.run()

if __name__ == '__main__':
    unittest.main()
