# mlops_project
# Sujet 8

# TO RUN THE PROJECT:

## RUN data_generate_script.py in the data_generation folder
This will generate the data of ATM transactions as a json ("data_generation/data.json").

## RUN KAFKA
### In your kafka/bin run the following commands:

./zookeeper-server-start.sh ../config/zookeeper.properties

./kafka-server-start.sh ../config/server.properties
kafka-topics.sh --bootstrap-server localhost:9092 --topic numtest --create --partitions 1 --replication-factor 1

### Next run the producer and consumer:

kafka/producer.py

kafka/consumer.py

Kafka will store the data read by the consummer as csv in the "atm_csvs" folder (it will create csvs after every 20000 transactions read by the consumer).
  
## RUN AIRFLOW FOR THE ANALYSIS
Since I am the one supposed to run airflow and show it with my account, the best we can do for someone else to see that I did implement airflow is by running the following command:


sudo spark-submit --total-executor-cores 4 --executor-cores 2 --executor-memory 5g --driver-memory 5g --name analysis_atm data_analysis/analysis.py

Or simply run the analysis.py file without airflow.

The results of the analysis are found in the "data_analysis/df_csvs" folder.

## RUN STREAMLIT
streamlit run data_analysis/streamlit_analysis.py
