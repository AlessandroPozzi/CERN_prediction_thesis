Alessandro Pozzi, Lorenzo Costantini

This project contains the code and the material that is the base of our thesis:
"Fault analysis of a complex electrical distribution system with Bayesian networks and Markov chains".

The project is composed by the following folders:
/src (with source code)
/res (with the device data extracted from the log)
	/debug (used by clustering methods)
/output (for the processed results)
	/dataframes (for storing the training set data of the Bayesian networks)
	/columnAnalysis (for additional analysis on data. May not be updated)
	/postProcessingAnalysis (for additional analysis on data. May not be updated)
	/preProcessingAnalysis (for additional analysis on data. May not be updated)
/material (for raw data, like the event log)

Composition of the source code folder:
/src
	/clustering 
	/globalcontrol (config file)
	/helpers
	/itemsetsgeneration (I phase for Bayesian networks)
	/markovchain (to generate all the Markov chains: I and II phase)
	/networkgeneration (II phase for Bayesian networks)
	/newdata
  
Main external dependencies:
- Pgmpy (Be sure to install the right version of the libraries - see pgmpy page)
- Graphviz (0.8.1)
- Pandas (0.20.3)
- Scikit-learn (0.19.1)
- Pomegranate (0.8.1)

---------------------------------------- Instructions ----------------------------------------

The generation of the models (Bayesian networks BN and Markov chains MC) is divided in two phases.

The first phase consists in extracting the training set (i.e. the itemsets or the sequences) from the log provided by the CERN. The log we used is about events happened in the year 2016 and it is a .csv file ("electric.csv.zip") that have been exported to a Mysql database. All the code that performs this first phase access to this database, so you have to set it up correctly (check the calls to mysql.connector.connect) in order to being able to run it.
For the BN, this phase must be done by running one of the "expandDevice..." modules in the package "itemsetsgeneration". This will generate, in the "/res" folder, a .txt file with the itemsets. Each "expandDevice" corresponds a  different way to preprocess data. The preprocessing with clustering must be done with the "expandDeviceClustering" module found in the "clustering" package.
For the MC, this phase is automatically performed when running the "main_markov" module, which will generate a .txt file containing the sequences.

The second phase consists in actually building the models (BN or MC) from the training sets generated in the previous phase. This will also generate the postprocessing analysis.
For the BN, you must run the module "main" in the "networkgeneration" package.
For the MC, you just have to run the "main_markov" module (that will also perform automatically the first phase).

Note: BN and MC phases are different and may present incoherences because of many tests, changes and scope alterations performed during the thesis. We are sorry for that.

The variations to the preprocessing, model generation and postprocessing phases presented in the thesis can be done by changing the parameters in the "main", "main_markov" and "config" modules. Refer to the documentation and the thesis.



