from ..step import Step
from ...utils import random_string, load_df, execute
from ...constants import SCHEMA_TEST, SCHEMA_PROD
from ..table import Table


class CustomTransformStep(Step):
    def __init__(self, custom_sql, redshift:bool=False, redshift_schema:dict=None, sf_user=None, **inputs):
        super().__init__()
        # assert all kwargs are steps
        self.custom_sql = custom_sql
        self.redshift = redshift
        self.redshift_schema = redshift_schema
        self.sf_user = sf_user
        self.inputs = inputs

    def __repr__(self):
        return f'\n\tCustomTransformStep(custom_sql:{self.custom_sql},\n\t\tstep_id:{self.step_id},\n\t\tinputs:{self.inputs},\n\t\tredshift:{self.redshift},\n\t\tredshift_schema:{self.redshift_schema})'

    def _execute(self, cache, debug, test):
        inputs = {k: self.inputs[k].execute(cache, debug, test) for k in self.inputs}
        schema = f'{SCHEMA_TEST}' if test else f'{SCHEMA_PROD}'

        final_inputs = inputs

        output = f"{schema}.{random_string()}_custom"
        sql = self.custom_sql(output, **final_inputs)
        print(f"""  
========== EXECUTING STEP: CUSTOM TRANSFORM
Step ID                 {self.step_id}
Inputs                  {', '.join(final_inputs.values())}
Transform               {sql}
Output:                 {output}
""")
        if not debug:
            execute(sql)
        return output

    def get_parent_step(self):
        if len(self.inputs) == 0:
            return None
        return self.inputs

    def update_parent_step(self, new_parent, key=None):
        if key in self.inputs.keys():
            self.inputs[key] = new_parent
        else:
            raise KeyError(f"Invalid key: {key} for this CustomTransform Step")

    def is_equal(self, other, cache, debug, test) -> bool:
        return (self.step_id == other.step_id) or (
            isinstance(other, CustomTransformStep)
            and self.compare_parent_keys(other)
            and self.custom_sql == other.custom_sql
            and set(self.inputs.keys()) == set(other.inputs.keys())
            and all([self.inputs[k].is_equal(other.inputs[k], cache, debug, test) for k in self.inputs.keys()])
            and self.redshift == other.redshift
        )
