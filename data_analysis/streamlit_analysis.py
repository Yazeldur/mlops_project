import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import streamlit as st
from PIL import Image
import pandas as pd

iconatm = Image.open("/home/yassinb/ING3/mlops/Yass/img/atm.png")
st.set_page_config(page_title="MLOPS",
                   page_icon=iconatm,
                   layout="wide")

st.title("MLOPS - Analyse de distributeur automatique de billets", anchor=None)

atm_path = "/home/yassinb/ING3/mlops/Yass/atm_csvs"

def most_least_transactions_per_hour():
    '''
    À quelle heure y a-t-il le plus et le moins de transactions ?
    '''
    df = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question1" + "/*.csv")))
    st.dataframe(df)
    st.write(f'L heure où l on enregistre le plus grand nombre de transactions sur tous les DAB est à {df["hour"].iloc[-1]}h avec {df["count"].iloc[-1]} transactions.')
    st.write(f'L heure où l on enregistre le moins de transactions sur tous les DAB est à {df["hour"].iloc[0]}h avec {df["count"].iloc[0]} transactions.')

def most_least_argent_retire():
    '''
    Quel est le montant le plus élevé et le plus bas de l'argent_retire à partir d'un distributeur automatique de billets (DAB)?
    '''
    df_min = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question2a" + "/*.csv")))
    df_max = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question2b" + "/*.csv")))
    st.write(f'La plus grande quantité d argent retiré est de {df_max["argent_retire"].iloc[0]}€.')
    st.write(f'La plus petite quantité d argent retiré est de {df_min["argent_retire"].iloc[0]}€.')

def highest_lowest_argent_retire_per_hour_all_ATMs():
    '''
    À quelle heure la somme d'argent_retire est-elle la plus élevée et la plus basse pour tous les DAB ?
    '''
    df = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question3" + "/*.csv")))
    st.dataframe(df)
    st.write(f'L heure à laquelle les sommes d argents sont les plus élevées pour tous les DAB est à {df["hour"].iloc[-1]}h avec {df["sum(argent_retire)"].iloc[-1]}€.')
    st.write(f'L heure à laquelle les sommes d argents sont les plus faibles pour tous les DAB est à {df["hour"].iloc[0]}h avec {df["sum(argent_retire)"].iloc[0]}€.')

def highest_lowest_argent_retire_per_hour_per_ATM():
    '''
    À quelle heure la somme d'argent retiré est-elle la plus élevé et le plus bas pour chaque DAB ?
    '''
    df_min = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question4a" + "/*.csv")))
    df_max = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question4b" + "/*.csv")))
    with st.container():
        dataframe_max, dataframe_min = st.columns(2)
        with dataframe_max:
            st.table(df_max)
        with dataframe_min:
            st.table(df_min)

def highest_lowest_argent_retire_per_day_all_ATMs():
    '''
    Quel jour la somme d'argent retiré est-elle la plus élevée et le plus basse pour tous les DAB ?")
    '''
    df = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question5" + "/*.csv")))
    st.dataframe(df)
    st.write(f'Le jour avec la somme d argent retiré la plus élevée est le {df["day"].iloc[-1]} avec {df["sum(argent_retire)"].iloc[-1]}€.')
    st.write(f'Le jour avec la somme d argent retiré la plus basse est le {df["day"].iloc[0]} avec {df["sum(argent_retire)"].iloc[0]}€.')

def highest_lowest_argent_retire_per_day_each_ATM():
    '''
    Quel jour la somme d'argent retiré est-elle la plus élevée et la plus basse pour chaque DAB ?
    '''
    df_min = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question6a" + "/*.csv")))
    df_max = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question6b" + "/*.csv"))) 

    with st.container():
        dataframe_max, dataframe_min = st.columns(2)
        with dataframe_max:
            st.table(df_max)
        with dataframe_min:
            st.table(df_min)

def main():
    st.subheader("1. À quelle heure y a-t-il le plus et le moins de transactions ? ")
    most_least_transactions_per_hour()
    
    st.subheader("2. Quel est le montant le plus élevé et le plus bas de l'argent_retire à partir d'un distributeur automatique de billets (DAB)?")
    most_least_argent_retire()
    
    st.subheader("3. À quelle heure la somme d'argent_retire est-elle la plus élevée et la plus basse pour tous les DAB ?")
    highest_lowest_argent_retire_per_hour_all_ATMs()

    st.subheader("À quelle heure la somme d'argent retiré est-elle la plus élevé et le plus bas pour chaque DAB ?")
    highest_lowest_argent_retire_per_hour_per_ATM()
    
    st.subheader("Quel jour la somme d'argent retiré est-elle la plus élevée et la plus basse pour tous les DAB ?")
    highest_lowest_argent_retire_per_day_all_ATMs()
    
    st.subheader("Quel jour la somme d'argent retiré est-elle la plus élevée et la plus basse pour chaque DAB ?")
    highest_lowest_argent_retire_per_day_each_ATM()

main()