fp=open("E:\\vmware\\cluster32-test.csv")
rd=fp.readlines()
wp=open("E:\\vmware\\cluster32-test-fv.csv","w")
X=[[0]*32 for i in range(0,len(rd)/100)]
counter=0
for each in rd:
    if(len(each.strip())>0):
        w=each.strip().split(',')
        X[counter/100][int(w[0].split('_')[-1])]+=1
        counter+=1

for i in X:
    line=""
    for j in i:
        line+="%d,"%j
    wp.write("%s\n"%line[:-1])
wp.close()
