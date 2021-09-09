import os
import sys

import panel as pn

sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("../../.."))

from entropylab.results.dashboard.panel_dashboard import Dashboard  # noqa: E402

pn.extension()

dashboard = Dashboard()
dashboard.servable()

# TODO : Remove this file (replaced by CLI/main.py)


def main():
    print("Hello World!")
    pn.serve(dashboard.servable())
