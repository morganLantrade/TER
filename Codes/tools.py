import random
import pandas as pd
import math
import os
import csv
from memory_profiler import profile
import linecache
import matplotlib.pyplot as plt
import numpy as np


#EXEMPLE DE TEMPS DES ACTIONS ( A MODIFIER POUR COLLER AUX PERFORMANCES DU PC)
COMP=0.003
HASH=0.009
MOVE=0.020
SWAP=0.060
IO=30


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

def generate_db(Rsize,Ssize,selectivity,double=False):
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

    if double:
        RY=[random.randint(1,Rsize) for _ in range(Rsize)]
        uniqueRY=list(set(RY))
        
        relation=int(Rsize*selectivity) 

        #on récupere les Y en communs aux hasard
        SY=[random.sample(uniqueRY,1)[0] for _ in range(relation)]

        #on récupere des y inexistants dans R de facon aléatoire et uniforme
        L=[random.randint(-(Ssize//2),0) for _ in range((Ssize//2)+1)]
        L+=[random.randint(Rsize+1,Rsize+(Ssize//2)) for _ in range((Ssize//2)+1)]

        SY+=random.sample(L,Ssize-relation) 
        random.shuffle(SY)

    else:
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

def test_cartesian_product(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    T,th_read,th_written,exp_read,exp_written=cartesian_product(R,S,selectivity,memory,size_of_tuple,size_of_page)
    print("-"*10)
    print("Cartesian")
    print("-"*10)
    print("R")
    print(R)
    print("*"*5)
    print("S)")
    print(S)
    print("*"*5)
    print("T")
    print(T)
    print("----") 
    print("Entrée/Sortie theorique : \n")
    print('Lecture :',th_read,'/ Ecriture :',th_written)
    print("----")
    print("Entrée/Sortie experimentale: \n")
    print('Lecture :',exp_read,'/ Ecriture :',exp_written)
    print("--")

def test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read,written=cartesian_product_index(R,S,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    print("-"*10)
    print("Cartesian")
    print("-"*10)
        
    print("----") 
    print("Theorique Build")
    print('Lecture :',read_build_th,'/ Ecriture :',written_build_th)
    print("----")
    print("Theorique Probe")
    print('Lecture :',read_probe_th,'/ Ecriture :',write_probe_th)
    print("----")
    print("Experimental Build: \n")
    print('Lecture :',read_build_exp,'/ Ecriture :',written_build_exp)
    print("----")
    print("Experimental Probe: \n")
    print('Lecture :',read,'/ Ecriture :',written)
    print("---")
    
def test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    
    ##############################
    #sort-merge
    ##############################
    print("-"*10)
    print("Sort-merge")
    print("-"*10)
    
    T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp=sort_merge_join(R,S,selectivity,memory,size_of_tuple,size_of_page)
    print("R")
    print(R)
    print("*"*5)
    print("S)")
    print(S)
    print("*"*5)
    print("T")
    print(T)
    print("----") 
    print("Theorique Build")
    print('Lecture :',read_build_th,'/ Ecriture :',written_build_th)
    print("----")
    print("Theorique Probe")
    print('Lecture :',read_probe_th,'/ Ecriture :',write_probe_th)
    print("----")
    print("Experimental Build: \n")
    print('Lecture :',read_build_exp,'/ Ecriture :',written_build_exp)
    print("----")
    print("Experimental Probe: \n")
    print('Lecture :',read_probe_exp,'/ Ecriture :',written_probe_exp)
    print("---")

def test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    print("-"*10)
    print("Hybrid Hash Join")
    print("-"*10)
    T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp=hybrid_hash_join(R,S,selectivity,memory,size_of_tuple,size_of_page)
    print(T)
    print("----") 
    print("Theorique Build")
    print('Lecture :',read_build_th,'/ Ecriture :',written_build_th)
    print("----")
    print("Theorique Probe")
    print('Lecture :',read_probe_th,'/ Ecriture :',write_probe_th)
    print("----")
    print("Experimental Build: \n")
    print('Lecture :',read_build_exp,'/ Ecriture :',written_build_exp)
    print("----")
    print("Experimental Probe: \n")
    print('Lecture :',read_probe_exp,'/ Ecriture :',written_probe_exp)
    print("---")



def plot_courbes(M,listes,cost=1,legende=["Sort_merge","Simple_Hash","Grace_Hash","Hybride_Hash","IndexR cartesian","IndexS cartesian"]):
    # Vérification des longueurs des listes
    longueur = len(listes[0])
    if any(len(lst) != longueur for lst in listes):
        raise ValueError("Les listes doivent avoir la même longueur.")

    
    for i in range(len(listes)):
        listes[i]=np.array(listes[i])[:,cost]
        plt.plot(M, listes[i], label=legende[i])

    # Ajout des légendes et du titre
    plt.legend()
    plt.xlabel("Memoire (M)")
    plt.ylabel("Cost "+"IO" if cost==0 else "Temps(ms)")
    plt.title("Comparaisons des couts des différents algorithmes")

    # Affichage du graphique
    plt.show()
    
   

