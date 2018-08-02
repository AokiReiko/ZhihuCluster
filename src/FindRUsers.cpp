#include<bits/stdc++.h>
using namespace std;

const int maxn = 52;
const int k = 30;

double a[maxn], c[1010][maxn];
vector <pair<double, string> > List;
map <string, int> TagMap;
set <string> Set;
char ch[200], user[200], name[200];
char tmp1[200], tmp2[200], tmp3[200], tmp4[200], tmp5[200], val[200];
wchar_t text[5000010];
map <string, string> answer;

int main() {
	double len = 0;
	for (int i = 0; i < 50; i ++) {
		scanf("%lf", &a[i]);
		len += a[i] * a[i];
	}
	len = sqrt(len);
	for (int i = 0; i < 50; i ++) {
		a[i] /= len;
	}
	cout << "=====" << endl;

	auto knn = fopen("../data/trainingset.txt", "r");
	for (int i = 0; i < 1000; i ++) {
		fscanf(knn, "%s", name);
		double dist = 0;
		for (int j = 0; j < 50; j ++) {
			fscanf(knn, "%lf", &c[i][j]);
			dist += c[i][j] * a[j];
		}
		fscanf(knn, "%s", ch);
		string tag = ch + 1;
		List.emplace_back(dist, tag);
	}
	sort(List.begin(), List.end());

	int MAX = 0;
	string LastTag = "";
	for (int i = List.size() - 1; i >= List.size() - k; i --) {
		cout << List[i].second << endl;
		if (TagMap.count(List[i].second)) {
			TagMap[List[i].second] = TagMap[List[i].second] + 1;
		} else {
			TagMap[List[i].second] = 1;
		}
		if (TagMap[List[i].second] > MAX) {
			MAX = TagMap[List[i].second];
			LastTag = List[i].second;
		}
	}

	cout << LastTag << endl;
}