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
  
Main external dependencies:
- Pgmpy (Be sure to install the right version of the libraries - see pgmpy page)
- Graphviz (0.8.1)
- Pandas (0.20.3)
- Scikit-learn (0.19.1)
- Pomegranate (0.8.1)

---------------------------------------- Instructions ----------------------------------------

The generation of the models (Bayesian networks BN and Markov chains MC) is divided in two phases.

The first phase consists in extracting the training set (i.e. the itemsets or the sequences) from the log provided by the CERN. The log we used is about events happened in the year 2016 and it is a .csv file ("electric.csv.zip") that have been exported to a Mysql database. All the code that performs this first phase access to this database, so you have to set it up correctly (check the calls to mysql.connector.connect) in order to being able to run it.
For the BN, this phase must be done by running one of the "expandDevice..." modules in the package "itemsetsgeneration". This will generate, in the "/res" folder, a .txt file with the itemsets. Each "expandDevice" corresponds to a different way to preprocess data.
The preprocessing with clustering must be done with the "expandDeviceClustering" module found in the "clustering" package. If you don't want to apply clustering, use "expandDevice.py". "expandDevice2.py" should not be used, "expandDeviceMultiRefDev" manages the case of multiple reference devices, "expandDeviceClusteringTestGraph" has been used for the generation of the graphs for the event distributions after or before different minutes from the reference device events. "expandDeviceTimestamp" has to be called if you want to use CERN's timestamp for the validation, where "validation" consists in doing the analysis starting from a set of timestamps rather than some reference device.

For the MC, this phase is automatically performed when running the "main_markov" module, which will generate a .txt file containing the sequences. If you want to use clustering for MC, you should run main_markov and set the clustering method you want to use in the config.py, which should be different from "no_clustering". There's not a separate expandDevice for clustering for MC's to be run as for BN's. "expandDeviceMarkov2.py" is never called. If config.Timestamp is set to True "expandDeviceMarkovTimestamp.py" will be called automatically, otherwise "expandDeviceMarkov.py" is called by main_markov.

The second phase consists in actually building the models (BN or MC) from the training sets generated in the previous phase. This will also generate the postprocessing analysis.
For the BN, you must run the module "main" in the "networkgeneration" package. The value of config.FILE_SUFFIX determines which .txt will be considered for the BN generation.
For the MC, you just have to run the "main_markov" module (that will also perform automatically the first phase).

Note: BN and MC phases are slightly different and may present incoherences because of many tests, changes and scope alterations performed during the thesis. We are sorry for that.

The variations to the preprocessing, model generation and postprocessing phases presented in the thesis can be done by changing the parameters in the "main", "main_markov" and "config" modules. Refer to the documentation and the thesis.

-------------------------------- Additional notes ------------------------------------

There are some references in the code to a "variance" criterion. They are not correct, because in the code we compute the standard deviation, as we have said in the thesis. Also, when we write about "lift" in the code, we are instead referring to the confidence criterion presented in our thesis. This inconsistency is due to the fact that variance and lift were initially two methods we tried, but later changed into standard deviation and confidence, so not all references might be updated.
