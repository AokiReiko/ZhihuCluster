import os

L = 0;
t = input()
a = t.split(" ")
for i in range(50):
	a[i] = float(a[i])
	L += a[i] * a[i];

L = pow(L, 0.5);
for i in range(50):
	a[i] /= L;

tag = ""
MAX = -1

fp = open("../data/trainingset.txt", "r");
for line in fp:
	S = line.strip().split("\t")
	d = S[1].split(" ")
	d[50] = d[50][1:]
	dist = 0
	for j in range(50):
		d[j] = float(d[j])
		dist += d[j] * a[j];
	if dist > MAX:
		MAX = dist
		tag = d[50]

lis = {}
fp = open("../data/wholeusers.txt", "r")
for line in fp:
	S = line.split("\t")
	if S[0] == tag :
		lis[S[1].strip()] = 1

ans = {}

cnt = 18
fp = open("../data/part1", "r")
for line in fp:
	S = line.split("\t")
	if S[0] in lis :
		ans[S[1]] = 1

title = {}
fp = open("../data/question_infos.txt", "r", encoding="utf-8")
for line in fp:
	S = line.split("\t")
	if len(S) < 7: 
		continue
	title[S[0]] = S[6]

fp = open("../data/answer_info.txt", "r", encoding="utf-8")
for line in fp:
	S = line.split("\t")
	if S[0] in ans:
		cnt -= 1
		print(title[S[1]], "\n       ", S[7][:50], "....\n\n")
		if cnt == 0 :
			break

