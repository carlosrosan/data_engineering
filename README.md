# data_engineering
Hi, my name is Carlos Rodriguez. You can contact me at carlosd2.rodriguez@gmail.com if you have any comments.

Here's some data engineering sample code of a late project of mine.

API_GET is a project that consist in quering an endpoint, using a python 3 script app_get.py and it's private config stored in the config.json.

The objective of this project was to query customer support cases of an external provider for data warehouse ingestion, as .csv files.

The entities that can be queried are case transactions such as cases and their interactions (activities), dynamic dimmensions such as users (contacts/consumers) and static dimmensions such as case types.

The script allows to set up how long back the user needs to query data, how frequent, and where to store it.

The proyect also consist in the data warehouse ingestion of the .csv output files, but that can be made using an ETL software solution, suchn as ADF, SSIS, and others.
