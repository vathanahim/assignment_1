from deliverables.steps.step import Step
from deliverables.steps import transform


def get_step_tables(s: Step):
    if isinstance(s, transform.FastPipelineCustomStep):
        output = [list(s.output_tables.values())[0]]
    else:
        output = list(s.output_tables.values())
    if isinstance(s, transform.PrecomputeTimescaling):
        output = []
        if hasattr(s, 'combined_input'):
            output.append(s.combined_input)
        if hasattr(s, 'combined_input_torun'):
            output.append(s.combined_input_torun)
    if isinstance(s, transform.WorkforceDynamicsPipeline):
        if getattr(s, 'created_input_table', False):
            output.append(s.input_table.prod_name)
        if getattr(s, 'created_append_table', False):
            output.append(s.append_table.prod_name)

    real_output = []
    for t in output:
        if t == '' or t is None:
            print('empty output found')
            print(s)
        else:
            if type(t) is str:
                valid_schemas = ['PROD_RUN_TABLES', 'TEST_RUN_TABLES',
                                 'PROD_PROMOTE_TABLES', 'TEST_PROMOTE_TABLES']
                if t.split('.')[0].upper() == 'SERVICE_15TH_AUTOMATION' and t.split('.')[1].upper() in valid_schemas:
                    real_output.append(t)
            else:
                print(f'WARNING! Step {s.step_id} has an invalid item in its output {t}')
    return real_output