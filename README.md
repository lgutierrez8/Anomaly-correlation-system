# Anomaly-correlation-system
Anomaly correlation system for a dynamic risk management environment 

The dynamic risk management in cybersecurity allows operators to have an adequate and dynamically updated cybersecurity awareness level based on changes in the organization’s environment. To achieve this, the system must be able to collect all the variations in a risk management environment (actives and threat variations) to assess the associated risk dynamically.

This project will deal with the detection of anomalies that raise as a result of a correlation of simpler anomalies. In these scenarios, it is expected to have a large number of heterogeneous sensors which can detect specific anomalies related to the recollected data. However, finding correlations between anomalies of heterogeneous sensors can be a clear symptom of risk variation due to a threat that uses different attack vectors, each one detected by its specific sensor. 

Related to these issues, this project will cover the following topics:

-Identify the different correlation mechanisms applicable to the scenario environments.

-Design and prototype the correlation mechanisms chosen for the generated data by the sensors in those scenarios.

-Validate the data with datasets generated by the sensors and anomalies individually detected in each datasheet.

-Use the results of these correlations to generate more significant anomalies and integrate them into an autonomous expert system based on ontologies that will evaluate the dynamic risk and alert if there is a major anomaly that could be interpreted as an attack.

-----------------------------------------------------------------------------------------------------------------
In this repository exists 3 different scripts:

-final.py: This is the final code of the project.

-test.py: This is the code for the 2 tests created.

-graphics.py: This is the code to obtain all the graphs used in the work memory.
