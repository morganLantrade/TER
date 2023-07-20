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
    
    I,O,cost_build,cost_probe=cartesian_product_index_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Index-cartesian :    I/O : {I}/{O}     | Time Build : {"X" if cost_build is None else round(cost_build,2)} sec   | Time Probe : {"X" if cost_probe is None else round(cost_probe,2)} sec | Total Time: {"X" if cost_probe is None else round((cost_build+cost_probe),2)} sec')
    print("--")

    I,O,cost_build,cost_probe=sort_merge_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Sort-merge :    I/O : {I}/{O}     | Time Build : {"X" if cost_build is None else round(cost_build,2)} sec   | Time Probe : {"X" if cost_probe is None else round(cost_probe,2)} sec | Total Time: {"X" if cost_probe is None else round((cost_build+cost_probe),2)} sec')
    print("--")

    I,O,cost_build,cost_probe=simple_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Simple Hash :    I/O : {I}/{O}     | Time Build : {"X" if cost_build is None else round(cost_build,2)} sec   | Time Probe : {"X" if cost_probe is None else round(cost_probe,2)} sec | Total Time: {"X" if cost_probe is None else round((cost_build+cost_probe),2)} sec')
    print("--")

    I,O,cost_build,cost_probe=grace_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Grace Hash :    I/O : {I}/{O}      | Time Build : {"X" if cost_build is None else round(cost_build,2)} sec   | Time Probe : {"X" if cost_probe is None else round(cost_probe,2)} sec | Total Time: {"X" if cost_probe is None else round((cost_build+cost_probe),2)} sec')
    print("--")

    I,O,cost_build,cost_probe=hybrid_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
    print(f'Hybrid Hash:    I/O : {I}/{O}      | Time Build : {"X" if cost_build is None else round(cost_build,2)} sec   | Time Probe : {"X" if cost_probe is None else round(cost_probe,2)} sec | Total Time: {"X" if cost_probe is None else round((cost_build+cost_probe),2)} sec')
    
    print("--"*10)

def results_cost_join(nbTupleR,nbTuplesS,selectivity,M,pageSize,Mode):
    
    if 1<= MODE[Mode][0]<=2:
        R= [ [] for _ in range(3)]
        for memory in M:
             I,O,cost_build,cost_probe = sort_merge_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[0].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = grace_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[1].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = cartesian_product_index_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[2].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             
    else:
        R= [ [] for _ in range(6)]
        for memory in M:
             I,O,cost_build,cost_probe = sort_merge_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[0].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = simple_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[1].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = grace_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[2].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = hybrid_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[3].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = cartesian_product_index_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[4].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = cartesian_product_index_cost2(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[5].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))

    return R

if __name__ == '__main__':
    help_mode= ["I","O","IO","Build Cost","Probe Cost","Cost"]
    Rsize=32*1001
    Ssize=Rsize*2
    selectivity=1
    memory=1000
    pageSize=32
    folderName="Run3"
    nbTuplesR,nbTuplesS=nb_tuples(folderName,"R",pageSize),nb_tuples(folderName,"S",pageSize)
    Mode= help_mode[5]
    
    '''
        
    #Generation de données

    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    db_to_file(R,pageSize,folderName,"R")
    db_to_file(S,pageSize,folderName,"S") 

    '''

    #test
    
    M= [m for m in range(40,4000,20)]
    for i in range(1000,1010):
        print(i)
        test_cost_join(Rsize,Ssize,selectivity,i,pageSize)
    R=results_cost_join(Rsize,Ssize,selectivity,M,pageSize,Mode)
    plot_courbes(M,R,Mode)
    
    
  