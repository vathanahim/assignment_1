# Part 2: Deliverables

In this part, you will be using a deliverable automation package that has already been built. It's a system that contains steps (in `deliverables/src/deliverables/steps`) which are standardized data operations (load / transform / extract). It also contains configurations (in `deliverables/src/deliverables/clients`): each client has their own configuration which defines their data deliveries, as a sequence of steps.

The expectation for this section is that you create a new step to generate a "Transitions" deliverable, based on the `transition_unique_base` table you created in Part 1, and add deliveries that use that step to a client file.

The system is complex, and you are not expected to go deep into any of it. Your ability to make a valuable contribution to an unfamiliar system, without necessarily having to understand all of it, is what will be tested.

Here is a brief overview on how to make the deliverable automation work:

- Make sure your MySQL server is running
- Create a virtual Python environment with the packages in `requirements.txt`. Python 3.9 is recommended.
- Edit `src/deliverables/utils/sql.py` > `db_client` function and bake in your MySQL password (also change the user if you're not using root)
- Call `python src/main.py --runmode=prod --deliverables=assignment --frequency=monthly --number_of_threads=20`
- Call `python src/main.py --runmode=prod --deliverables=assignment --frequency=monthly --number_of_threads=20 --promote`

If this runs successfully end-to-end, the system is installed correctly. Additionally, this should create the following table: `DELIVERABLE_OUTPUT.INDIVIDUAL_POSITION`.

By executing this command, you asked the system to run the client "assignment" (ie run the config defined by the `deliverables/src/deliverables/clients/assignment.py` client file). That config has a single delivery, which is a delivery of an `IndividualPosition` step to the above location in MySQL.

If you look at the usage of `IndividualPosition` in the file, you'll see that it takes an input table argument, the table in question being defined in a `CustomTransform` step above. This is because our clients are generally interested in only a subset of companies, and not the whole universe of profiles. In this case, the `IndividualPosition` step takes in our input file (which states that we are only interested in seeing users from the companies Jane Street and Revelio Labs) and filters the `individual_position` file based on the company IDs we provide. 

This assignment is fairly open-ended, so make sure to add comments to explain your reasoning and assumptions.

### A. Create Transition step

In a new file, define a `Transition` class (inheriting the `Step` type) which will source from the `transition_unique_base` table, and an input table with companies. Here are the instructions:
- It should create two outputs: an inflow file and an outflow file. The inflow file should contain all transitions going INTO positions that are part of a company in the input table. The outflow file should contain all transitions going OUT OF positions that are part of a company in the input table. 
- Only transitions between 2 different companies should be included in the outputs. A transition where the user changed positions within the same company should be excluded.
- There should be a `columns` argument like in `IndividualPosition`, but for this step, the user should only have to specify the "basic names" of the columns. For instance, if the user specifies `city` as a column, the output files for this step should have `prev_city` and `new_city` included. You should implement logic in the step to derive the `prev_` and `new_` version of these columns (if they exist!) and correctly get them from the source table

You are heavily encouraged to mimic the structure of the `IndividualPosition` step, when it comes to defining the class and its methods.
The last 3 methods (`get_parent_step`, `update_parent_step` and `is_equal`) are required to make the script work, but copying the existing ones into the new step should be enough (make sure to change the mention of the class name in `is_equal`).

### B. Create Transition step tests

In a new file in `deliverables/src/deliverables/steps/transform/tests`, create unit tests for the transition step you just created. Again, you should draw inspiration from the tests that are already set up for `IndividualPosition`. Make sure that pytest is installed so you can run it locally and verify that it works.

### C. Add transition deliveries to the config

In the `deliverables/src/deliverables/clients/assignment.py` configuration files, add a Transition step leading into 2 deliveries: `INFLOW_FILE` and `OUTFLOW_FILE`. Hint: to access different output files of the same step, use the [] operator. 


### FAQ:

- In the CopySQL step which delivers the data to the client, there is a prod location and a test location, for the purpose of being able to test the delivery works without actually delivering it. Here, it doesn't matter, so we use the same for both.
- Same thing when defining a `table.Table` (a SQL table input) inside the transform step definitions

### assignment result
I was struggling with this because I couldn't get my code to run properly and tried looking at the error logs """mysql.connector.errors.ProgrammingError: 1146 (42S02): Table 'PROD_PROMOTE_TABLES.DELIVERABLE_OUTPUT_INDIVIDUAL_POSITION_copy_sql' doesn't exist
 """, which i'm not sure why as I don't understand the code base. 