# Part 1: Ingestion
The goal of this part is to start from `position_history`, a table that has the history of all scrapes we've received for certain profiles, and to produce tables that reflect the current state of the profiles, with extra enriched data columns joined in.
## Relevant Tables
| Table Name                                        | Description |
|---------------------------------------------------| ----------- |
| `ingestion.position_history`   | A table containing all historical scraped position data.        | 
| `ingestion.location_mapper_v3` | A modeling output table that contains granular mapped raw location information.
| `ingestion.company2rcid`       | A modeling output table that contains a mapping from company name to RCID (Revelio Company ID).

Please note that for questions A and B, the tables we are asking you to create are already present in the database. You can use them as sanity checks to make sure you are doing the work correctly, but you are still expected to give us a script that recreates them.

For all these problems, we expect a .sql file, or a Python script that executes SQL through the MySQL connector. Please use as many comments as you see fit to explain your work.

### A. Create Position Current
Given the `position_history` table above, please create a `position_current` table, which contains only the "current" position data for each user, disregarding historical scrapes. It should have the same columns as `position_history`.

### B. Create Position Base Table 
Given the `position_current` table you just created, please create `individual_position`, taking all the fields from the raw scrape and adding new ones from the modeling tables: `location_mapper_v3` and `company2rcid`. 

### C. Create Transitions Base Table
Another dataset clients are interested in are users' job transitions between positions, meaning successive positions that a user holds. Please define and explain what should count as a transition, then implement the table. Starting from `individual_position`, please create the `transition_unique_base` table. 

The expected format of the table is that each row should contain information about two successive positions. For each field in `individual_position`, if the field is specific to a position, it should appear twice per row: once with a `prev_` prefix and once with a `new_` (referring to the "previous position" and "new position" in the transition). If a field is specific to a user (ex: `user_id`), it should only appear once. 

### Assignment result
I created a cloud database using Aiven mysql the connection to this database is the following:
`mysql -h mysql-c314d3b-reveliolab-assignment1.e.aivencloud.com -P 24878 -u avnadmin -p  --ssl-mode=REQUIRED` password is `AVNS_4T0M1r4SdTxtoZAMUPL`

For the purpose of this assignment passwords and .crt are exposed so reviewers can easily access it.

To access the database via bash the following script can be used: `mysql -h mysql-c314d3b-reveliolab-assignment1.e.aivencloud.com -P 24878 -u avnadmin -p  --ssl-mode=REQUIRED`

I created  `db_setup.py` to run a script to create different tables from `db_setup.sql`

I also created a bash script to insert a much larger sql file into my db called `ingest_insert.sh` for the purpose of speeding up large volume ingest 

I created  `part1_ingest_script.ipynb` to extract data from the db into dataframes, do transformations, and insert it back into the database. I find it faster and easier to do manipulation via pandas or similar dataframes rather then doing it via SQL which in some cases my include many joins, groups and etc.. 
