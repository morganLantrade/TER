import random
import pandas as pd
import math
import os
from tools import *

def cartesian_product_file(folderName,memory,pageSize):
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    nbPageR=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])

    b=memory-2 #taille bloc
    
    T=[]
    path='Data/'+folderName+"_cp"
    
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        delete_file("T",folderName+"_cp") #supprime tous les fichiers du repertoire faudra l'enleve pour faire les bonnes mesures
    

    nbPageT=0
    i=0
    while i<nbPageR:
        
        if i+b< nbPageR:
            R=read_X_pages(folderName+"/R",i+1,b)
        elif i==0: 
            #cas ou tout R rentre dans la mémoire
            R=read_X_pages(folderName+"/R",i+1,nbPageR)
        else: 
            #cas ou on charge les dernieres pages
            R=read_X_pages(folderName+"/R",i+1,nbPageR%i)
            i=nbPageR
        i+=b
                
        for j in range (nbPageS):
            S=read_X_pages(folderName+"/S",j+1,1)
            for k in range(len(R.index)):
                for l in range(len(S.index)):
                    if (R["Y"].get(k)==S["Y"].get(l)):
                        T.append((R['X'].get(k),R['Y'].get(k),S['Z'].get(l)))
                        if len(T)==pageSize:
                            pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)
                            nbPageT+=1
                            T=[]
    if T:
        pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)






def cartesian_product(R,S,selectivity,memory,size_of_tuple,size_of_page):
    '''Renvoi un  join des tables R et S en utilisant un algorithme de produit cartésien par block'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"
    
    
    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_= number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple
    b=memory-2 #taille bloc
    th_read= R_pages  + S_pages*math.ceil(R_pages/b)
    th_written= math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)
    
        
    ##Experiment
    n=len(R)
    m=len(S)
    T=[]
    written=1 # dans tous les cas on ecrira sur au moins une page output
    read=0 

    for i in range(n):
        if i%tuples_per_page==0: #page suivante de R 
            read+=1 
        for j in range(m):
            if (i%(tuples_per_page*b)==0) and (j%tuples_per_page==0):  #bloc de b pages suivant donc on compte les pages de S lues
                read+=+1
            if R['Y'].get(i)==S['Y'].get(j):
                T.append((R['X'].get(i),R['Y'].get(i),S['Z'].get(j)))
                written+= int(len(T)%tuples_per_page==0)  #page suivante de T
                
    return pd.DataFrame(T,columns=['X','Y','Z']),th_read,th_written,read,written

def cartesian_product_index(R,S,selectivity,memory,size_of_tuple,size_of_page,size_key_index):
    '''Renvoi une simulation memoire d'un join des tables R et S en utilisant un algorithme de produit cartésien indexe sur S'''
    assert memory>=4, "Erreur : La memoire doit contenir au moins 4 pages"
    
    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,idx=number_of_pages(S,size_of_tuple,size_of_page,"B-arbre",size_key_index)
    tuples_per_page= size_of_page//size_of_tuple
    
    #Build
    read_build_th = S_pages 
    written_build_th = sum(idx.values()) #nombre pages index

    #Probe
    n=len(idx) # nombre de niveau a charger regulierement
    free_space=memory-2 
    read=0
    #tant qu'on peut stocker les niveaux de l'index dans la mémoire on réduit le nombre de niveau a charger
    while free_space-idx[(n-1)]>=1 and n>0:  
        free_space-= idx[(n-1)]
        read+=idx[(n-1)] # pages des niveaux que l'on charge qu'une seule fois
        n-=1
    
    
    
    read_probe_th = R_pages + len(R)*(n+1) + read  #niveau a charger + lecture effective de S sur le disque
    write_probe_th = written= math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)
    


    ##Experiment
    print("Pages par niveau de l'index :",idx)

    # A implementer un index B-arbre ou juste simulation theorique de l'index
    read_build_exp=read_build_th
    written_build_exp = written_build_th

    
    for i in range(len(R)):
        if i%tuples_per_page==0: #page suivante de R 
            read+=1 
        j=0
        while j<= n: #on ne charge que les niveaux a charger
            read +=1 
            j+=1 
    
    return read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read,written
