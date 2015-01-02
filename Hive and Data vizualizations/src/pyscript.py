 wp=open("graphhhu.txt","w+")
 for each in d.keys():
     line=""
     line="%s,%s,%s\n"%(each[0],each[1],d[each])
     wp.write(line)
  wp.close()
 wp=open("graphhhu.txt","r")
 rd=wp.readlines()
 wp1=open("filtered.txt","w+")
 for each in rd:
     w=each.split(",")
     if int(w[-1])100:
             wp1.write(each)
  wp1.close()

