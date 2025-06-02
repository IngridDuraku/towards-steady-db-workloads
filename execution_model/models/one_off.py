from execution_model.models.base import BaseExecutionModel
from execution_model.utils.const import ExecutionTrigger


class OneOffExecutionModel(BaseExecutionModel):
    def __init__(self, wl):
        super().__init__(wl)

    def generate_workload_execution_plan(self):
        if self.wl_execution_plan is None:
            self.wl_execution_plan = self.wl
            self.wl_execution_plan["execution"] = "normal"
            self.wl_execution_plan["execution_trigger"] = ExecutionTrigger.IMMEDIATE.value
            self.wl_execution_plan["triggered_by"] = self.wl_execution_plan["query_hash"]

        return self.wl_execution_plan

    def get_cost(self, hw_parameters):
        return super().get_cost(
            hw_parameters,
        )