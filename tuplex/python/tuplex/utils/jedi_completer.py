#!/usr/bin/env python3
# ----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
# ----------------------------------------------------------------------------------------------------------------------#

from typing import Any, List

from jedi import Interpreter, settings
from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document


class JediCompleter(Completer):
    """REPL Completer using jedi"""

    def __init__(self, get_locals: Any) -> None:
        # per default jedi is case insensitive, however we want it to be case sensitive
        settings.case_insensitive_completion = False
        self.get_locals = get_locals

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> List[Completion]:
        _locals = self.get_locals()
        interpreter = Interpreter(document.text, [_locals])

        # Jedi API changed, reflect this here
        completions = []
        if hasattr(interpreter, "completions"):
            completions = interpreter.completions()
        elif hasattr(interpreter, "complete"):
            completions = interpreter.complete()
        else:
            raise Exception(
                "Unknown Jedi API, please update or install older version (0.18)"
            )

        for completion in completions:
            if completion.name_with_symbols.startswith("_"):
                continue

            yield Completion(
                completion.name_with_symbols,
                len(completion.complete) - len(completion.name_with_symbols),
            )
