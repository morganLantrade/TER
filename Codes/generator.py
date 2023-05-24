import random
import pandas as pd



def generate_db(N,selectivity,double=False):
    relation=int(N*selectivity)
    R=[]
    S=[]
    if double:
        RY=[random.randint(1,9) for _ in range(N)]
    else:
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
    iR=0
    iS=0
    while iS<N and iR<N :
        if R['Y'].get(iR)==S['Y'].get(iS):
            T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
            iS+=1
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
        else:
            iS-=1
            while R['Y'].get(iR)==S['Y'].get(iS):
                iS-=1
            iS+=1
            iR+=1
    return pd.DataFrame(T,columns=['X','Y','Z'])
    
def cartesian_product(R,S,cond="R.Y=Z.Y"):
    if cond=="R.Y=Z.Y":
        n=len(R)
        m=len(S)
        T=[]
        for i in range(n):
            for j in range(m):
                if R['Y'].get(i)==S['Y'].get(j):
                    T.append((R['X'].get(i),R['Y'].get(i),S['Z'].get(j)))
    return pd.DataFrame(T,columns=['X','Y','Z'])


if __name__ == '__main__':
    N=10
    selectivity=0.8
    R,S=generate_db(N,selectivity,double=True)

    print(R.sort_values(by=['Y']))
    print()
    print(S.sort_values(by=['Y']))
    print()
    print(sort_merge(R,S))

    '''print(R)
    print()
    print(S)
    print()
    print(cartesian_product(R,S))'''

    #print(R.join(S.set_index('Y'), on='Y',how='inner'))