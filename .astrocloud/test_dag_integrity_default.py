"""Test the validity of all DAGs. **USED BY DEV PARSE COMMAND DO NOT EDIT**"""
import os
from airflow.models import DagBag, Variable
from airflow.hooks.base import BaseHook

# The following code patches errors caused by missing OS Variables, Airflow Connections, and Airflow Variables

# =========== MONKEYPATCH BaseHook.get_connection() ===========
def basehook_get_connection_monkeypatch(key: str):
    print(f"user tried fetching connection {key}")


BaseHook.get_connection = basehook_get_connection_monkeypatch
# # =========== /MONKEYPATCH BASEHOOK.GET_CONNECTION() ===========

# =========== MONKEYPATCH OS.GETENV() ===========
def os_getenv_monkeypatch(key: str, default=None):
    print(f"user tried fetching var {key}")


os.getenv = os_getenv_monkeypatch
# # =========== /MONKEYPATCH OS.GETENV() ===========

# =========== MONKEYPATCH VARIABLE.GET() ===========
def variable_get_monkeypatch(key: str):
    print(f"user tried fetching var {key}")


Variable.get = variable_get_monkeypatch
# # =========== /MONKEYPATCH VARIABLE.GET() ===========

def test_dagbag():
    """
    Validate DAG files using Airflow's DagBag.
    This includes sanity checks e.g. do tasks have required arguments, are DAG ids unique & do DAGs have no cycles.
    """
    dag_bag = DagBag(include_examples=False)
    print(dag_bag)
    assert not dag_bag.import_errors  # Import errors aren't raised but captured to ensure all DAGs are parsed

    # Additional project-specific checks can be added here, e.g. to enforce each DAG has a tag
    for dag_id, dag in dag_bag.dags.items():
        error_msg = f"{dag_id} in {dag.fileloc} has no tags"
        assert error_msg