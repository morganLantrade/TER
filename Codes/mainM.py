from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *
from tools import *
import time

#Oral Mercredi 30 aout 13h30


def test_cost_join(nbTupleR,nbTuplesS,selectivity,memory,pageSize):
    print("--"*10)
    r,w,c=cartesian_product_index_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Index-cartesian :    I/O : {r}/{w}     | Time : {"X" if c is None else round(c/1000,2)} sec')
    r,w,c=sort_merge_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Merge-sort :    I/O : {r}/{w}     | Time : {"X" if c is None else round(c/1000,2)} sec')
    print("--")
    r,w,c=simple_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Simple-Hash :    I/O : {r}/{w}     | Time : {"X" if c is None else round(c/1000,2)} sec')
    print("--")
    r,w,c=grace_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Grace-sort :    I/O : {r}/{w}     | Time : {"X" if c is None else round(c/1000,2)} sec')
    print("--")
    r,w,c=hybrid_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Hybride-sort :    I/O : {r}/{w}     | Time : {"X" if c is None else round(c/1000,2)} sec')
    print("--"*10)

def results_cost_join(nbTupleR,nbTuplesS,selectivity,M,pageSize):
    R= [ [] for _ in range(6)]
    for memory in M:
         r,w,c = sort_merge_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
         R[0].append((r+w,c))
         r,w,c = simple_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
         R[1].append((r+w,c))
         r,w,c = grace_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
         R[2].append((r+w,c))
         r,w,c = hybrid_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
         R[3].append((r+w,c))
         r,w,c = cartesian_product_index_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
         R[4].append((r+w,c))
         r,w,c = cartesian_product_index_cost2(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
         R[5].append((r+w,c))

    return R

if __name__ == '__main__':
    
    Rsize=32*1001
    Ssize=Rsize*2
    selectivity=1
    memory=7
    pageSize=32
    folderName="Run3"
    nbTuplesR,nbTuplesS=nb_tuples(folderName,"R",pageSize),nb_tuples(folderName,"S",pageSize)

    
    '''
        
    #Generation de données

    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    db_to_file(R,pageSize,folderName,"R")
    db_to_file(S,pageSize,folderName,"S") 

    '''

    #test
    
    M= [m for m in range(40,4000,20)]

    R=results_cost_join(Rsize,Ssize,selectivity,M,pageSize)
    plot_courbes(M,R,0)
    
    
  