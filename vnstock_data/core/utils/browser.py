import requests
def get_cookie(url,headers=None):
	A=headers
	if A is None:A={'Accept':'application/json, text/plain, */*','Accept-Language':'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7','Connection':'keep-alive','User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'}
	B=requests.get(url,headers=A,verify=True)
	if B.status_code==200:
		C=B.headers.get('Set-Cookie')
		if C:D=C;return D
		else:print("No 'Set-Cookie' header found.")
	else:print(f"Error: {B.status_code}")