from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *

#Oral Mercredi 30 aout 13h30

def time_test(folderName,pageSize,LMemory,repetition,RunName):

    clean_data(folderName)

    print("["+" "*50+"] 0%", end="\r")

    seconds=time.time()
    nbOfTests=repetition*5*len(LMemory)
    LTime=[]
    iTest=0
    for memory in LMemory:
        LMean=[[] for _ in range(5)]
        for i in range(repetition):
            LMean[0].append(sort_merge_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")
            LMean[1].append(simple_hash_join_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")
            LMean[2].append(grace_hash_join_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")
            LMean[3].append(hybrid_hash_join_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")
            LMean[4].append(cartesian_product_index_file(folderName,memory,pageSize))
            iTest+=1
            percentage=round(((iTest/nbOfTests)*100))
            print("["+"■"*(percentage//2)+" "*(50-(percentage//2))+"] "+str(percentage)+"%", end="\r")
        
        LTime.append((memory,sum(LMean[0])/repetition,sum(LMean[1])/repetition,sum(LMean[2])/repetition,sum(LMean[3])/repetition,sum(LMean[4])/repetition))
    T=pd.DataFrame(LTime,columns=["Memory"]+LEGENDS[:-1])
    T.to_csv('TimeTest/'+folderName+"_"+RunName+".csv",sep=',',index=False)
    print("["+"■"*50+"] 100%")
    timer=round(time.time()-seconds)
    second=timer%60
    minute=((timer-seconds)//60)%60
    heure=(((timer-seconds)//60)-minute)//60
    print("Run done in : "+ str(heure)+"h "+str(minute)+"m "+str(second)+"s")

if __name__ == '__main__':
    
    Rsize=(10*32)-5
    Ssize=Rsize*2
    selectivity=1
    memory=250
    pageSize=32
    folderName="R101S202Sel1"
    LMemory=[i for i in range (13,52,3)]+[i for i in range (52,103,5)]+[i for i in range (103,210,15)]+[210,210]
    repetition=3
    RunName="grosrunsamèr"
    

    
    #Generation de données

    #R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    #db_to_file(R,pageSize,folderName,"R")
    #db_to_file(S,pageSize,folderName,"S")

    #db=read_X_pages(folderName+"/R",1,1)
    #L=[]
    #--------Test--------
    
    #L.append((db["Y"].get(0),db["X"].get(0)))
    #print(type(L[0][0]))

    #sort_file(folderName,memory,pageSize,"R")

    #time_test(simple_hash_join_file,folderName,pageSize,LMemory)


    #Théorique

    #print(cartesian_product(folderName,selectivity,memory,pageSize))
    #print(sort_merge_join(folderName,selectivity,memory,pageSize))

    #Pratique

    # timer=cartesian_product_index_file(folderName,memory,pageSize)
    # print("cartesian Done in : "+ str(timer)+"s")
    # timer2=sort_merge_file(folderName,memory,pageSize) 
    # print("sort merge Done in : "+ str(round(timer2,2))+"s")
    # timer3=simple_hash_join_file(folderName,memory,pageSize)
    # print("simple hash Done in : "+ str(round(timer3,2))+"s")
    # timer4=grace_hash_join_file(folderName,memory,pageSize)
    # print("grace hash Done in : "+ str(round(timer4,2))+"s")
    # timer5=hybrid_hash_join_file(folderName,memory,pageSize)
    # print("hybrid hash Done in : "+ str(round(timer5,2))+"s")
    time_test(folderName,pageSize,LMemory,repetition,RunName)
    #time.sleep(5)



    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    #test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    #test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    #test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    
