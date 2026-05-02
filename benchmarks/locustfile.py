from locust import HttpUser, task

class LoadTest(HttpUser):
    @task
    def test_lock(self):
        self.client.post("/lock/exclusive/node1")