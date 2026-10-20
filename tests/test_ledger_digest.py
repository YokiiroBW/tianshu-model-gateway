"""Strict migration comparisons independent of source/target collation."""

import sqlite3
import unittest

from tianshu_gateway.ledger_storage import row_digest


class LedgerDigestTests(unittest.TestCase):
    def test_different_database_order_and_permutations_preserve_digest(self):
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE turns(service TEXT,turn_id TEXT,version INTEGER)")
            connection.executemany(
                "INSERT INTO turns VALUES(?,?,?)",
                [("companion", "a", 7), ("companion", "B", 8), ("companion", "角色", 9)],
            )
            query = "SELECT service,turn_id,version FROM turns ORDER BY turn_id COLLATE "
            binary = connection.execute(query + "BINARY").fetchall()
            nocase = connection.execute(query + "NOCASE").fetchall()
            self.assertNotEqual(binary, nocase)
            expected = row_digest(iter(binary))
            self.assertEqual(expected, row_digest(iter(nocase)))
            self.assertEqual(expected, row_digest(iter(reversed(binary))))
            self.assertEqual(expected, row_digest(connection.execute(query + "NOCASE")))

    def test_same_count_content_and_value_type_changes_are_rejected(self):
        original = [("companion", "same-id", '{"outcome":"completed"}', 7)]
        expected = row_digest(iter(original))
        for changed in (
            [("companion", "same-id", '{"outcome":"unknown"}', 7)],
            [("companion", "same-id", '{"outcome":"completed"}', 8)],
            [("companion", "same-id", '{"outcome":"completed"}', "7")],
            [("companion", "same-id", '{"outcome":"completed"}', 7.0)],
            [("companion", "same-id", '{"outcome":"completed"}', None)],
        ):
            with self.subTest(changed=changed):
                actual = row_digest(iter(changed))
                self.assertEqual(actual[0], expected[0])
                self.assertNotEqual(actual[1], expected[1])

    def test_duplicate_rows_preserve_multiplicity_and_count(self):
        first, second = ("companion", "first", 7), ("companion", "second", 8)
        original = row_digest(iter([first, first, second]))
        replaced = row_digest(iter([first, second, second]))
        self.assertEqual(original[0], replaced[0])
        self.assertNotEqual(original[1], replaced[1])
        removed = row_digest(iter([first, second]))
        self.assertEqual(original[0], 3)
        self.assertEqual(removed[0], 2)
        self.assertNotEqual(original[1], removed[1])
        self.assertEqual(original, row_digest(iter([second, first, first])))

    def test_empty_input_has_stable_zero_row_digest(self):
        empty = row_digest(iter(()))
        self.assertEqual(empty, row_digest(iter([])))
        self.assertEqual(empty[0], 0)
        self.assertEqual(len(empty[1]), 32)
        self.assertNotEqual(empty, row_digest(iter([(None,)])))


if __name__ == "__main__":
    unittest.main()
