import unittest

from ror_scripts.get_ultimate_parent import roll_up, traverse_parents


class TestGetUltimateParent(unittest.TestCase):
    def test_roll_up(self) -> None:
        id_to_parent = {"A": "B", "B": "C", "C": "D", "D": "D", "E": "E"}
        expected_id_to_ultimate_parent = {
            "A": "D",
            "B": "D",
            "C": "D",
            "D": "D",
            "E": "E",
        }
        self.assertEqual(expected_id_to_ultimate_parent, roll_up(id_to_parent))

    def test_traverse_parents(self) -> None:
        id_to_parent = {"A": "B", "B": "C", "C": "D", "D": "D"}
        self.assertEqual("D", traverse_parents("A", id_to_parent))
        self.assertEqual("D", traverse_parents("D", id_to_parent))
