import random

f = open('numberTest', 'a')
for i in range(0, 500):
	num = random.randint(1535,1540)
	f.write(str(num) + '\n')
f.close()