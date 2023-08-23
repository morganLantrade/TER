import random
import pandas as pd
import math
import os
import csv
from memory_profiler import profile
import linecache
import matplotlib.pyplot as plt
import numpy as np
import time


#EXEMPLE DE TEMPS DES ACTIONS ( A MODIFIER POUR COLLER AUX PERFORMANCES DU PC) en ms
#COMP=0.003
#HASH=0.009
#MOVE=0.020
#SWAP=0.060
#IO=30
IO_READ=0.089
IO_WRITE=0.787
COMP=0.0004
HASH=0.0006
MOVE=0.001
SWAP=0.003

LEGENDS = ["Sort_merge","Simple_Hash","Grace_Hash","Hybride_Hash","IndexR cartesian","IndexS cartesian"]
BUILD_LEGENDS = ["Sort_merge","Grace_Hash","IndexR cartesian"]
RESULT_FOLDER_NAME={"hybrid_hash_join_file":"_hybrid_hash","grace_hash_join_file":"_grace_hash","simple_hash_join_file":"_hash","cartesian_product_index_file":"_cpi","sort_merge_file":"_sm"}


MODE= { "I" : (0,LEGENDS) , "O" : (1,LEGENDS),"IO" : (2,LEGENDS), "Build Cost" : (3,BUILD_LEGENDS) , "Probe Cost": (4,BUILD_LEGENDS) , "Cost" : (5,LEGENDS) ,"Experimental" : (6,LEGENDS[:-1]) }

def unit_test(algo,folderName,LMemory,pageSize):
    for memory in LMemory:
        clean_data(folderName)
        algo(folderName,memory,pageSize)
        assert test_result(folderName,folderName+RESULT_FOLDER_NAME[algo.__name__]) , "Fail at memory ="+str(memory)
    print("Unit test completed successfully")

def test_result(folderName,resultFolderName):

    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if ("S") in f])
    nbPageT=len([f for f in os.listdir("Data/"+resultFolderName) if ("T") in f])

    dbR=read_X_pages(folderName+"/R",1,nbPageR)
    dbS=read_X_pages(folderName+"/S",1,nbPageS)

    dbT=read_X_pages_T(resultFolderName+"/T",1,nbPageT).sort_values(by=['Y']).reset_index(drop=True)
    dbT2=pd.merge(dbR, dbS, left_on='Y', right_on='Y', how='left').sort_values(by=['Y']).reset_index(drop=True)

    return dbT.equals(dbT2)

def meanOfList(L):
    return sum(L)/len(L)

def indexOfMin(L):
    index=0
    min=L[0]
    for i in range(1,len(L)):
        if L[i]<min:
            min=L[i]
            index=i
    return index


def read_result(folderName,name):
    '''Retourne une liste de tuple correspondant aux resultats des algos en temps(sec)'''
    L=[]
    with open("TimeTest/"+folderName+"_"+name+".csv", newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            label=next(spamreader)
            for a,b,c,d,e,f in spamreader:
                L.append((int(a),float(b),float(c),float(d),float(e),float(f)))
    db=pd.DataFrame(L,columns=label)
    results= [ [] for _ in range(5)]
    M= db["Memory"].values.tolist()         
    for i in range(len(LEGENDS)-1):
        results[i]= db[LEGENDS[i]].values.tolist()      
    return M,results


def read_line(name,page,line):
    '''Retourne le tuple correspondant selon la ligne et la page'''
    path="Data/"+name+"_"+str(page)+".csv"
    x,y =linecache.getline(path, line).rstrip().split(',')

    return int(x),int(y)

    
def read_X_pages(name,i,x):
    '''Retourne un dataframe correspondant au fichier name de la page i jusqu'a la page i+x'''
    L=[]
    for i in range(i,i+x):
        with open("Data/"+name+"_"+str(i)+".csv", newline='') as csvfile:

            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            a=next(spamreader)
            for x,y in spamreader:
                L.append((int(x),int(y)))
                
    return pd.DataFrame(L,columns=a)

def read_X_pages_T(name,i,x):
    '''Retourne un dataframe correspondant au fichier name de la page i jusqu'a la page i+x'''
    L=[]
    for i in range(i,i+x):
        with open("Data/"+name+"_"+str(i)+".csv", newline='') as csvfile:

            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            a=next(spamreader)
            for x,y,z in spamreader:
                L.append((int(x),int(y),int(z)))
                
    return pd.DataFrame(L,columns=a)

def read_X_pages_legacy(name,i,x):
    '''Retourne un dataframe correspondant au fichier name de la page i jusqu'a la page i+x'''
    db=pd.read_csv('Data/'+name+"_"+str(i)+".csv")
    k=1
    L=[db]
    while k!=x:
        path='Data/'+name+"_"+str(i+k)+".csv"
        assert os.path.exists(path), f'Erreur : Le fichier {path} n existe pas'
        L.append(pd.read_csv(path))
        k+=1
    db=pd.concat(L, axis=0,ignore_index=True)
    
    return db

def clean_data(folderName):
    if os.path.exists('Data/'+folderName+"_grace_hash_partition"):
        #vide le contenu
        for f in os.listdir("Data/"+folderName+"_grace_hash_partition"):
            delete_folder(folderName+"_grace_hash_partition/"+f)
        os.rmdir("Data/"+folderName+"_grace_hash_partition")
    if os.path.exists('Data/'+folderName+"_hybrid_hash_partition"):
        #vide le contenu
        for f in os.listdir("Data/"+folderName+"_hybrid_hash_partition"):
            delete_folder(folderName+"_hybrid_hash_partition/"+f)
        os.rmdir("Data/"+folderName+"_hybrid_hash_partition")
    for f in os.listdir("Data"):
        if folderName+"_" in f:
            delete_folder(f)

def delete_folder(folderName):
    delete_file("csv",folderName)
    os.rmdir("Data/"+folderName)

def delete_file(dbName,folderName):
    '''Supprime tous les fichiers de folrderName contenante dbNamr'''
    for f in os.listdir("Data/"+folderName):
        if dbName in f:
            os.remove("Data/"+folderName+"/"+f)

    

def db_to_file(db,pageSize,folderName,dbName):
    '''Ecrit dans la run foldername, les fichiers dbName en découpant le dataframe db en pages selon la taille des pages et le contenu de db'''

    n=len(db.index)
    nb_of_files=n//pageSize
    if not os.path.exists('Data/'+folderName):
        os.makedirs('Data/'+folderName)
    #supprime les versions precedentes de dbName
    delete_file(dbName,folderName) 
    #parcours des pages   
    for i in range(nb_of_files):
        db_temp=db.iloc[i*pageSize:(i+1)*pageSize]
        db_temp.to_csv('Data/'+folderName+"/"+dbName+"_"+str(i+1)+".csv",sep=',',index=False)
    #derniere page non pleine
    if (n%pageSize!=0):
        db_temp=db.iloc[nb_of_files*pageSize:]
        db_temp.to_csv('Data/'+folderName+"/"+dbName+"_"+str(nb_of_files+1)+".csv",sep=',',index=False)

def generate_db(Rsize,Ssize,selectivity):
    '''Renvoi deux dataframe correspondant aux tables R(X,Y) et S(Y,Z)
    N : nombre d'élements dans chaque table
    selectivity : compris entre 0 et 1 , permet de déterminer le pourcentage
                  d'Y en communs de la table S et celle de R
    double : doublon possible ou non
    '''

    R=[]
    S=[]

    switch=False

    if Rsize>Ssize:
        Rsize,Ssize=Ssize,Rsize
        switch=True

    
    RY=[i for i in range(1,Rsize+1)]
    random.shuffle(RY)

    relation=math.ceil(Rsize*selectivity) 

    #on récupere les Y en communs aux hasard
    SY=random.sample(RY,relation)

    #on récupere des y inexistants dans R de facon aléatoire et uniforme

    L=[i for i in range(-Ssize,1)]
    L+=[i for i in range(Rsize+1,Rsize+Ssize+1)]

    SY+=random.sample(L,Ssize-relation) 
    random.shuffle(SY)

    if switch:
        RY,SY=SY,RY
        Rsize,Ssize=Ssize,Rsize

   
    #création des tables
    for i in range(Rsize):
        R.append((i+1,RY[i]))
    for i in range(Ssize):
        S.append((SY[i],i+1))
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    return R,S

def number_of_pages(dataframe,size_of_tuple,size_of_page,index="Not",size_key_index=8):
    '''Renvoie le nombre de pages de la table et la taille des différents niveau de l'index si index est B-arbre'''
    total_tuples=len(dataframe.index)
    Idx=dict()
    nb_tuples= size_of_page//size_of_tuple
    nb_pages= math.ceil(total_tuples/nb_tuples)
        
    if index=="B-arbre":
        i=0
        nb_tuples= size_of_page//(size_of_tuple//2+size_key_index) #on garde que l'attribut Y
        nb_pages_idx= math.ceil(total_tuples/nb_tuples)
        while nb_pages_idx>1:
            Idx[i]=nb_pages_idx
            nb_pages_idx= math.ceil(nb_pages_idx/nb_tuples)
            i+=1
        Idx[i]=1 #dernier niveau
    return nb_pages,Idx

def nb_tuples(folderName,dbName,pageSize):
    '''Retourne le nombre de tuple que contient dbName dans folrdername'''
    nbPage=len([f for f in os.listdir("Data/"+folderName) if dbName in f])
    return ((nbPage-1)*pageSize)+len(read_X_pages(folderName+"/"+dbName,nbPage,1).index)

def plot_courbes(M,lists,Mode="Cost"):

    title,(x,label) = Mode, MODE[Mode]
    

    length= len(lists[0])
    if any(len(lst) != length for lst in lists):
        raise ValueError("Les listes doivent avoir la même longueur.")

    
    for i in range(len(lists)):
        lists[i]=np.array(lists[i])
        if x!=6: #plusieurs dimension = theorique
            lists[i] = lists[i][:,x]
            plt.plot(M, lists[i], label=label[i])
        else: #experimental
            plt.plot(M, lists[i],"-p",markersize=3, label=label[i])

    # Ajout des légendes et du titre
    plt.legend()
    plt.xlabel("Memoire (M)")
    plt.ylabel(title + (" in sec" if x!=0 else ""))
    plt.title("Comparaisons des couts des différents algorithmes")

    # Affichage du graphique
    plt.show()
    
   

