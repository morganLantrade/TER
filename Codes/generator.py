import random
import pandas as pd



def generate_db(N,selectivity):
    relation=int(N*selectivity)
    R=[]
    S=[]
    RY=[i for i in range(1,N+1)]
    random.shuffle(RY)
    SY=random.sample(RY,relation)
    L=[]
    L=[i for i in range(-N+1,1)]
    L+=[i for i in range(N+1,2*N+1)]
    SY+=random.sample(L,N-relation)
    random.shuffle(SY)
    for i in range(N):
        R.append((i+1,RY[i]))
        S.append((SY[i],i+1))
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    return R,S

def sort_merge(R,S):
    n=len(R)
    R=R.sort_values(by=['Y'])
    S=S.sort_values(by=['Y'])
    R=R.reset_index(drop=True)
    S=S.reset_index(drop=True)
    T=[]
    iS=0
    iR=0
    while iS<N and iR<N :
        if R['Y'].get(iR)==S['Y'].get(iS):
            T.append((R['X'].get(iR),S['Z'].get(iS)))
            iS+=1
            iR+=1
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
        else:
            iR+=1
    return pd.DataFrame(T,columns=['X','Z'])
    

if __name__ == '__main__':
    N=10
    selectivity=0.8
    R,S=generate_db(N,selectivity)
    print(R)
    print()
    print(S)
    print()
    print(sort_merge(R,S))

    #print(R.join(S.set_index('Y'), on='Y',how='inner'))