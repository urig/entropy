import os
import sys

import panel as pn

from entropylab.results_backend.sqlalchemy.project import project_name

sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("../../.."))

from entropylab.results.dashboard.panel_dashboard import Dashboard  # noqa: E402


def serve_results(path: str, port: int = 0):
    pn.extension()
    dashboard = Dashboard(path)
    pn.serve(
        dashboard.servable(),
        port=port,
        title=f"'{project_name(path)}' - Entropy experiment results",
    )
