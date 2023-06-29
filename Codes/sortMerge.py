import random
import pandas as pd
import math
import os
from tools import *


def sort_file(folderName,memory,pageSize):
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    nbPageR=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])

    if not os.path.exists('Data/'+folderName+"_sorted"):
        os.makedirs('Data/'+folderName+"_sorted")

    
 
    

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


def sort_merge_join(R,S,selectivity,memory,size_of_tuple,size_of_page):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de tri fusion'''

    n=len(R)
    m=len(S)

    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_=number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple
    
    #Build
    read_build_th = R_pages*(1+math.ceil(math.log(math.ceil(R_pages/memory),memory-1)))+S_pages*(1+math.ceil(math.log(math.ceil(S_pages/memory),memory-1)))
    written_build_th = read_build_th

    #Probe
    read_probe_th=R_pages+S_pages
    write_probe_th=math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)

    # A implementer
    read_build_exp=read_build_th
    written_build_exp = written_build_th

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

    #return ,read1+read2,written1+written2,read,written
    

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