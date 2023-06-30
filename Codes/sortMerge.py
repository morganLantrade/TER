import random
import pandas as pd
import math
import os
from tools import *


def sort_file(folderName,memory,pageSize,dbName):
    '''Effectue un tri externe du fichier dbName de la run folderName selon la memoire et la taille des pages'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    #metadonnee
    nbPage=len([f for f in os.listdir("Data/"+folderName) if dbName in f])

    if not os.path.exists('Data/'+folderName+"_sorted"):
        os.makedirs('Data/'+folderName+"_sorted")
    else:
        #vide le contenu
        delete_file(dbName,folderName+"_sorted",)

    nbMonotonie=0
    passe=0

    ###########
    # PASSE 0 #
    ###########
    #parcours par bloc de taille memoire
    for i in range(0,nbPage,memory):
        #dernier bloc
        if (i==nbPage-(nbPage%memory)):
            db=read_X_pages(folderName+"/"+dbName,(nbPage-(nbPage%memory))+1,nbPage%memory)
            #tri quicksort de tout le bloc
            db=db.sort_values(by=['Y'],kind='quicksort')
            #ecriture du bloc en page
            db_to_file(db,pageSize,folderName+"_sorted",dbName+"0_"+str(nbPage//memory))
            nbMonotonie+=1
        #tous les blocs de taille memoire
        else:
            db=read_X_pages(folderName+"/"+dbName,i+1,memory)
            #tri quicksort de tout le bloc
            db=db.sort_values(by=['Y'],kind='quicksort')
            #ecriture du bloc en page
            db_to_file(db,pageSize,folderName+"_sorted",dbName+"0_"+str(i//memory))
            nbMonotonie+=1

    ####################
    # PASSES SUIVANTES #
    ####################
    while nbMonotonie!=1: #derniere passe
        nbPageMonotonie=(memory*((memory-1)**passe))
        #parcours des monotonies
        for i in range(math.ceil(nbMonotonie/(memory-1))):
            L=[]
            #regroupement de memoire-1 monotonies
            for j in range(memory-1):
                #si toutes les pages de R ont étées lues
                if nbPage>(j+i*(memory-1))*nbPageMonotonie:
                    #dernier bloc de monotonies
                    if (j+i*(memory-1)==nbMonotonie-1) and ((nbPage%(memory*((memory-1)**(passe))))!=0):
                        db=read_X_pages(folderName+"_sorted/"+dbName+str(passe)+"_"+str(j+i*(memory-1)),1,nbPage%nbPageMonotonie)
                    #tous les blocs de monotonies de taille normale
                    else:
                        db=read_X_pages(folderName+"_sorted/"+dbName+str(passe)+"_"+str(j+i*(memory-1)),1,nbPageMonotonie)
                    L.append(db)
            db=pd.concat(L, axis=0,ignore_index=True)
            #mergesort du bloc
            db=db.sort_values(by=['Y'],kind='mergesort',ignore_index=True)
            #ecriture du bloc trie en page
            db_to_file(db,pageSize,folderName+"_sorted",dbName+str(passe+1)+"_"+str(i))
        #passe suivante
        nbMonotonie=math.ceil(nbMonotonie/(memory-1))
        passe+=1
    return passe

def sort_merge_file(folderName,memory,pageSize):
    '''Effectue un join de S et R contenus dans la run foldername selon la memoire et la taille de page'''
    
    if not os.path.exists('Data/'+folderName+"_sm"):
        os.makedirs('Data/'+folderName+"_sm")
    else:
        #vide le contenu
        delete_file("T",folderName+"_sm",)

    #Tri les fichiers sur disque et revoie le nombre de passes pour chaque relation
    passeR=sort_file(folderName,memory,pageSize,"R")
    passeS=sort_file(folderName,memory,pageSize,"S")
    
    #metadonnees
    nbPageR=len([f for f in os.listdir("Data/"+folderName+"_sorted") if ("R"+str(passeR)) in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName+"_sorted") if ("S"+str(passeS)) in f])

    #Initialisation
    iPageR=1
    iPageS=1
    iT=1
    R=read_X_pages(folderName+"_sorted/R"+str(passeR)+"_0",iPageR,1)
    S=read_X_pages(folderName+"_sorted/S"+str(passeS)+"_0",iPageS,1)
    
    iS=0
    iR=0
    T=[]
    #Tant qu'on a pas fini de parcourir une relation
    while iPageR<nbPageR+1 and iPageS<nbPageS+1:
        #Tant qu'on a pas fini un page
        while (iS<len(S.index) and iR<len(R.index)):
            if R['Y'].get(iR)==S['Y'].get(iS): 
                T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
                #vide le buffer si plein
                if (len(T)==pageSize):
                    T=pd.DataFrame(T,columns=['X','Y','Z'])
                    T.to_csv('Data/'+folderName+"_sm/T_"+str(iT)+".csv",sep=',',index=False)
                    iT+=1
                    T=[]
                iS+=1
                iR+=1
                
            elif R['Y'].get(iR)>S['Y'].get(iS):
                iS+=1
                
            else:
                iR+=1
        #page S finie
        if iS==len(S.index):
            iPageS+=1
            #si la page existe on charge la suivante
            if iPageS<=nbPageS:
                S=read_X_pages(folderName+"_sorted/S"+str(passeS)+"_0",iPageS,1)
                iS=0
        #page R finie
        else:
            iPageR+=1
            #si la page existe on charge la suivante
            if iPageR<=nbPageR:
                R=read_X_pages(folderName+"_sorted/R"+str(passeR)+"_0",iPageR,1)
                iR=0
    #vide le buffer si non vide
    if T:
        T=pd.DataFrame(T,columns=['X','Y','Z'])
        T.to_csv('Data/'+folderName+"_sm/T_"+str(iT)+".csv",sep=',',index=False)

        


def sort_merge(arr,i,memory,tuples_per_page):
    '''Fonction qui appelle la fonction recursive merge qui renvoie la liste arr triée 
    et le nombre de lecture et ecriture dans la memoire'''
    read_count = 0

    if len(arr) <= 1:
        return arr, read_count
    mid = len(arr) // 2
    left_half = arr[:mid]
    right_half = arr[mid:]

    left_half, left_reads, left_writes = sort_merge(left_half,i)
    right_half, right_reads, right_writes = sort_merge(right_half,i)

    read_count += left_reads + right_reads

    sorted_arr, merge_reads, merge_writes = merge(left_half, right_half,i)
    read_count += merge_reads

    return sorted_arr, math.ceil(len(arr)/tuples_per_page)


def merge(left, right,i):
    '''FOnction recursive qui effectue un tri fusion avec mémorisation des lectures et écritures dans la mémoire
    selon i l'index pour lequel le tuple est trié  , 1 pour R et 0 pour S'''
    merged = []
    left_index = 0
    right_index = 0
    reads = 0
    writes = 0

    while left_index < len(left) and right_index < len(right):
        if left[left_index][i] <= right[right_index][i]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
        writes += 1

    while left_index < len(left):
        merged.append(left[left_index])
        left_index += 1
        writes += 1

    while right_index < len(right):
        merged.append(right[right_index])
        right_index += 1
        writes += 1

    reads += len(left) + len(right)
    return merged, reads, writes


def sort_merge_join(folderName,selectivity,memory,pageSize):
    '''Renvoi le nombre de lecture et ecriture disque theorique en utilisant un algorithme de tri fusion'''

   
    ##Theoric##
    #metadonnées
    R_pages=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    S_pages=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])
    nbTuplesR=((R_pages-1)*pageSize)+len(read_X_pages(folderName+"/R",R_pages,1).index)
    
    #Build
    read_build_th = R_pages*(1+math.ceil(math.log(math.ceil(R_pages/memory),memory-1)))+S_pages*(1+math.ceil(math.log(math.ceil(S_pages/memory),memory-1)))
    written_build_th = read_build_th

    #Probe
    read_probe_th=R_pages+S_pages
    write_probe_th=math.ceil(math.ceil(nbTuplesR*selectivity)/pageSize)

    '''

    # Tri des deux tables au préalable
    R,read1,written1=sort_merge(R.values.tolist(),1,memory,tuples_per_page)
    S,read2,written2=sort_merge(S.values.tolist(),0,memory,tuples_per_page)
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    T=[]
    iR=0
    iS=0
    written=0
    while iR<n and iS<m:
        if R['Y'].get(iR)==S['Y'].get(iS): 
            T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
            written+=1
            iS+=1
            iR+=1
            
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
            
        else:
            iR+=1
    
    read_probe_exp=math.ceil(iR/tuples_per_page)+math.ceil(iS/tuples_per_page)
    written_probe_exp=math.ceil(written/tuples_per_page)

    T=pd.DataFrame(T,columns=['X','Y','Z'])
    return T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp

    #return ,read1+read2,written1+written2,read,written'''
    return read_build_th+read_probe_th,written_build_th+write_probe_th
    

#algo sort_merge doublon
'''
while iR<n :
        read+=1 # le prochain Y
        if iS==m :
            if R['Y'].get(iR+1)==S['Y'].get(iS-1) :
                iS-=1
            
                while R['Y'].get(iR)==S['Y'].get(iS): #gère le cas des doublons
                    iS-=1
                    read+=1

                iS+=1
                iR+=1
                read+=2 #on incrémente des deux cotes
                
            else :
                
                return pd.DataFrame(T,columns=['X','Y','Z']),read1+read2,written1+written2,read,written
        elif R['Y'].get(iR)==S['Y'].get(iS): 
            T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
            written+=1
            iS+=1
            
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
            
        else:
            iS-=1
            
            while R['Y'].get(iR)==S['Y'].get(iS): #gère le cas des doublons
                iS-=1
                read+=1

            iS+=1
            iR+=1
            read+=2 #on incrémente des deux cotes

    T=pd.DataFrame(T,columns=['X','Y','Z'])
'''