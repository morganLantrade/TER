from sortMerge import *
from cartesian import *
from tools import *
from hash import *

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


if __name__ == '__main__':
    
    Rsize=321
    Ssize=650
    selectivity=1
    memory=40
    pageSize=32
    size_of_page=1024
    size_of_tuple=32
    size_key_index=8
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    #db_to_file(R,32,"Run1","R")
    #db_to_file(S,32,"Run1","S")
    cartesian_product_file("Run1",memory,pageSize)
    #sort_file("Run1",memory,pageSize)

    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    #test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    #test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    #test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    '''
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    b,c=number_of_pages(R,32,1024)
    print("Taille de R:")
    print("Nombre de pages : ",b)
    print("Index : ",c)

    print("---------")
    b,c=number_of_pages(S,32,1024,"B-arbre",32)
    print("Taille de S:")
    print("Nombre de pages : ",b)
    print("Index : ",c)
    
    ##############################
    #sort-merge
    ##############################
    print("-"*10)
    print("Sort-merge")
    print("-"*10)
    
    A,r0,w0,r1,w1=sort_merge_join(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',r0,'/ Ecriture :',w0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)
    ##############################
    #cartesien
    ##############################
    print("-"*10)
    print("Cartesian")
    print("-"*10)
   
    A,r0,w0,r1,w1=cartesian_product(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',0,'/ Ecriture :',0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)

    ##############################
    #hachage
    ##############################
    print("-"*10)
    print("Simple Hash")
    print("-"*10)

    A,r0,w0,r1,w1=hash_join(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',r0,'/ Ecriture :',w0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)'''