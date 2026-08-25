import unittest

from ferry.ai import DEFAULT_MODEL
from ferry.args_parser import get_parser


class ArgsParserTests(unittest.TestCase):
    def test_llm_model_help_uses_current_default(self) -> None:
        parser = get_parser()
        action = next(
            action for action in parser._actions if action.dest == "llm_model"
        )

        self.assertIn(DEFAULT_MODEL, action.help or "")
