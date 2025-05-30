from execution_model.models.base import BaseExecutionModel

class OneOffExecutionModel(BaseExecutionModel):
    def __init__(self, wl):
        super().__init__(wl)

    def generate_workload_execution_plan(self):
        if self.wl_execution_plan is None:
            self.wl_execution_plan = self.wl
            self.wl_execution_plan["execution"] = "normal"

        return self.wl_execution_plan

    def get_cost(self, hw_parameters):
        return super().get_cost(
            hw_parameters,
        )