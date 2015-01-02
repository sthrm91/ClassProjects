fp=open("C:\\Users\\bhuvi\\Downloads\\csv_input\\Test1_Input-app3.csv")
rd=fp.readlines()
wp=open("E:\\app3-test.csv","w")
for each in rd:
    if(len(each.strip())>0):
        w=each.strip().split(',')
        for i in range(0,100):
            wp.write("%s,%s,%s\n"%(w[i],w[i+100],w[i+200]))
wp.close()
