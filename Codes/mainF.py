from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *

#Oral Mercredi 30 aout 13h30

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

if __name__ == '__main__':

    pageSize=32
    Rsize=(101*pageSize)-5
    Ssize=Rsize*2
    selectivity=1
    memory=10
    folderName="R101S202P32Sel1"
    LMemory=[i for i in range (13,52,3)]+[i for i in range (52,103,5)]+[i for i in range (103,210,15)]+[210,211]
    #LMemory=[i for i in range (5,11)]+[i for i in range (11,28,4)]
    repetition=5
    RunName="gros_run_R5"
    
    #test_result("R11S22P256Sel1","R11S22P256Sel1_cpi")
    
    #Generation de données

    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    db_to_file(R,pageSize,folderName,"R")
    db_to_file(S,pageSize,folderName,"S")

    #db=read_X_pages(folderName+"/R",1,1)
    #L=[]
    #--------Test--------
    
    time_test_run(RunName,folderName,pageSize,LMemory,repetition)

    #Théorique



    #Pratique

    #timer=cartesian_product_index_file(folderName,memory,pageSize)
    #print("cartesian Done in : "+ str(timer)+"s")
    #timer2=sort_merge_file(folderName,memory,pageSize) 
    #print("sort merge Done in : "+ str(round(timer2,2))+"s")
    #timer3=simple_hash_join_file(folderName,memory,pageSize)
    #print("simple hash Done in : "+ str(round(timer3,2))+"s")
    #timer4=grace_hash_join_file(folderName,memory,pageSize)
    #print("grace hash Done in : "+ str(round(timer4,2))+"s")
    #timer5=hybrid_hash_join_file(folderName,memory,pageSize)
    #print("hybrid hash Done in : "+ str(round(timer5,2))+"s")

    #time.sleep(5)


    
