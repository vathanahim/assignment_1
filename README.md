# Revelio Labs Assignment - Data Scientist / Customer-facing Data Engineer

The purpose of this assignment is to recreate parts of the data pipeline that Revelio Labs uses to go from raw data to cleaned, formatted data, eventually delivered to clients. In this case, we will be focusing on profile data (from online platforms such as LinkedIn), and in particular on positions, which are jobs that a user has held and put on their profile.

The assignment has two parts. In the first part (ingestion), you will be working from a table that has a full history of data scrapes, to create tables that will be used as a base for client deliveries. In the second part (deliverables), you will be creating the client deliveries in question.

In order to be able to test your work throughout the assignment, you should have MySQL installed. The installation and setup of MySQL is at your discretion, and the way you go about it will be part of your evaluation. Please detail the steps you took in your submission.

Once you have a MySQL server up and running and you can connect to it, run the commands in db_setup.sql, which will create a few empty databases. Then, download this [database dump](https://info0.s3.us-east-2.amazonaws.com/assignment-ds-cfde/ingestion.sql) and import it into the newly created `INGESTION` database.

After all of this is set up, please move to the ingestion section of the assignment. Refer to the `README.md` file inside the `ingestion` folder. Good luck!