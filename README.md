# Project of Decision Support Systems - Laboratory of Data Science
This repository hosts our Decision Support Systems project. 
Here the detailed project descrition: [Project Assignment PDF](project_description.pdf)

Long story short:
In Part 1 of the project, we built and populated a data warehouse using various input files, with a primary focus on "Police.csv." This dataset contains information about gun violence incidents in the United States from January 2013 to March 2018, including details about victims, firearms, and locations. The project involves the following key tasks:

- Divide "Police.csv" into six tables: custody, gun, participant, date, incident, and geography.
- Data Integration: generation of missing IDs (e.g., participant ID and geo ID) for enhanced data completeness; computation of the crime gravity attribute for each incident using a specific formula; retrieval of the city and state of each incident, possibly with the integration of external data.


All data processing operations will be performed without using the pandas library.
The resulting datawarehouse will serve as the foundation for the subsequent phases of the project, facilitating in-depth analysis and insights.
