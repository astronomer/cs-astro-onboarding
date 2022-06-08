from airflow.providers.databricks.hooks.databricks import DatabricksHook

class DatabricksUtil:
    def __init__(self):
        self.hook = DatabricksHook()

    def run_now(self, job_id, notebook_params=None):
        payload = {
            "job_id": job_id,
            "notebook_params": notebook_params
        }
        self.hook.run_now(json=payload)

    def submit_run(self, json: dict):
        return self.hook.submit_run(json=json)

    def get_run_page_url(self, run_id: str):
        return self.hook.get_run_page_url(run_id=run_id)

    def get_job_id(self, run_id: str):
        return self.hook.get_job_id(run_id=run_id)

    def get_run_state(self, run_id: str):
        return self.hook.get_run_state_str(run_id=run_id)

    def cancel_run(self, run_id: str):
        return self.hook.cancel_run(run_id=run_id)

    def start_cluster(self, cluster_id: str):
        payload = {
            "cluster_id": cluster_id
        }
        return self.hook.start_cluster(json=payload)

    def restart_cluster(self, cluster_id: str):
        payload = {
            "cluster_id": cluster_id
        }
        return self.hook.restart_cluster(json=payload)

    def terminate_cluster(self, cluster_id: str):
        payload = {
            "cluster_id": cluster_id
        }
        return self.hook.terminate_cluster(json=payload)

    def install(self, json: dict):
        return self.hook.install(json=json)

    def uninstall(self, json: dict):
        return self.hook.uninstall(json=json)