import random
import pandas as pd
import math
import os
    
def read_X_pages(name,i,x):
    db=pd.read_csv('Data/'+name+"_"+str(i)+".csv")
    k=1
    while k!=x:
        path='Data/'+name+"_"+str(i+k)+".csv"
        assert os.path.exists(path), f'Erreur : Le fichier {path} n existe pas'
        db_tmp=pd.read_csv(path)
        db=pd.concat([db, db_tmp], axis=0,ignore_index=True)
        k+=1
    
    return db


def delete_file(dbName,folderName):
    for f in os.listdir("Data/"+folderName):
        if dbName in f:
            os.remove("Data/"+folderName+"/"+f)

    

def db_to_file(db,pageSize,folderName,dbName):

    n=len(db.index)
    nb_of_files=n//pageSize
    if not os.path.exists('Data/'+folderName):
        os.makedirs('Data/'+folderName)

    delete_file(dbName,folderName)    
    for i in range(nb_of_files):
        db_temp=db.iloc[i*pageSize:(i+1)*pageSize]
        db_temp.to_csv('Data/'+folderName+"/"+dbName+"_"+str(i+1)+".csv",sep=',',index=False)
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
