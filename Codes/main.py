from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *

def time_test_run(RunName,folderName,pageSize,LMemory,repetition):

    clean_data(folderName)

    print("["+" "*50+"] 0%", end="\r")

    seconds=time.time()
    nbOfTests=repetition*5*len(LMemory)
    LTimeMean=[]
    LTimeMin=[]
    LTimeMax=[]
    iTest=0

    for memory in LMemory:
        L=[[] for _ in range(5)]
        for i in range(repetition):

            L[0].append(sort_merge_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")

            L[1].append(simple_hash_join_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")

            L[2].append(grace_hash_join_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")

            L[3].append(hybrid_hash_join_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")

            L[4].append(cartesian_product_index_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")

        LTimeMean.append((memory,meanOfList(L[0]),meanOfList(L[1]),meanOfList(L[2]),meanOfList(L[3]),meanOfList(L[4])))
        LTimeMin.append((memory,min(L[0]),min(L[1]),min(L[2]),min(L[3]),min(L[4])))
        LTimeMax.append((memory,max(L[0]),max(L[1]),max(L[2]),max(L[3]),max(L[4])))

    TMean=pd.DataFrame(LTimeMean,columns=["Memory"]+LEGENDS[:-1])
    TMean.to_csv('TimeTest/'+folderName+"_"+RunName+"_mean.csv",sep=',',index=False)
    TMin=pd.DataFrame(LTimeMin,columns=["Memory"]+LEGENDS[:-1])
    TMin.to_csv('TimeTest/'+folderName+"_"+RunName+"_min.csv",sep=',',index=False)
    TMax=pd.DataFrame(LTimeMax,columns=["Memory"]+LEGENDS[:-1])
    TMax.to_csv('TimeTest/'+folderName+"_"+RunName+"_max.csv",sep=',',index=False)
    clean_data(folderName)
    print("["+"■"*50+"] 100%")
    timer=round(time.time()-seconds)
    second=timer%60
    minute=((timer-second)//60)%60
    heure=(((timer-second)//60)-minute)//60
    print("Run done in : "+ str(heure)+"h "+str(minute)+"m "+str(second)+"s")

    
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
    
    if 3<= MODE[Mode][0]<=4:
        R= [ [] for _ in range(3)]
        for memory in M:
             I,O,cost_build,cost_probe = sort_merge_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[0].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = grace_hash_join_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[1].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             I,O,cost_build,cost_probe = cartesian_product_index_cost(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             R[2].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))
             
    else:
        R= [ [] for _ in range(5)]
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
             #I,O,cost_build,cost_probe = cartesian_product_index_cost2(nbTupleR,nbTuplesS,selectivity,memory,pageSize)
             #R[5].append((I,O,I+O,cost_build,cost_probe,cost_build+cost_probe))

    return R

if __name__ == '__main__':

    pageSize=32                     #nombre de n-uplets par page de mémoire vive simulée
    rSize=(101*pageSize)-5          #nombre de n-uplets total de R
    sSize=rSize*2                   #nombre de n-uplets total de S (doit être plus grand que rSize)
    selectivity=1                   #selectivité
    memory=40                       #taille de la mémoire vive simulée (en nombre de pages)
    folderName="R101S202P32Sel1"    #Nom du dossier où les données générées sont enregistrées dans des fichiers .csv (dans le dossier Data)


    #Generation de données


    R,S=generate_db(rSize,sSize,selectivity)   #Génère les deux bases de données R et S
    db_to_file(R,pageSize,folderName,"R")      #Enregistre les données dans le dossier folderName (Le créé ou l'écrase si nécessaire)
    db_to_file(S,pageSize,folderName,"S")


    #Algorithmes de jointure


    timer=cartesian_product_index_file(folderName,memory,pageSize)      #Applique l'algorithme de jointure cartésien indexé sur les données stocker dans folderName 
    print("cartesian Done in : "+ str(timer)+"s")                       #et enregistre le résultat dans un dossier folderName + _cpi (L'index est stocker dans folderName + _idx2)

    timer2=sort_merge_file(folderName,memory,pageSize)                  #Applique l'algorithme de jointure tri-fusion sur les données stocker dans folderName 
    print("sort merge Done in : "+ str(round(timer2,2))+"s")            #et enregistre le résultat dans un dossier folderName + _sm (les deux bases de données triées sont stocker dans folderName + _sorted)

    timer3=simple_hash_join_file(folderName,memory,pageSize)            #Applique l'algorithme de jointure hachage simple sur les données stocker dans folderName 
    print("simple hash Done in : "+ str(round(timer3,2))+"s")           #et enregistre le résultat dans un dossier folderName + _hash (Si necessaire les données temporaires sont stocker dans folderName + _hash_temp)

    timer4=grace_hash_join_file(folderName,memory,pageSize)             #Applique l'algorithme de jointure hachage GRACE sur les données stocker dans folderName 
    print("grace hash Done in : "+ str(round(timer4,2))+"s")            #et enregistre le résultat dans un dossier folderName + _grace_hash (Les partitions sont stocker dans folderName + _grace_hash_partition)

    timer5=hybrid_hash_join_file(folderName,memory,pageSize)            #Applique l'algorithme de jointure hachage hybride sur les données stocker dans folderName 
    print("hybrid hash Done in : "+ str(round(timer5,2))+"s")           #et enregistre le résultat dans un dossier folderName + _hybrid_hash (Les partitions sont stocker dans folderName + _hybrid_hash_partition)
    