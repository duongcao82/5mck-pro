'\nRuntime License Verification for vnii\n\nProvides real-time license verification for vnstock packages.\nIncludes caching to minimize API calls.\n\nDesign:\n  - Verify license before package usage\n  - Cache verification results (1 hour TTL)\n  - Automatic retry on network failures\n  - Graceful degradation for offline usage\n'
_E='device_id'
_D='expiry'
_C='api_key'
_B=False
_A=True
import json,logging,os,time,requests
from pathlib import Path
from typing import Dict,Optional
log=logging.getLogger(__name__)
class LicenseCache:
	'Simple file-based cache for license verification results'
	def __init__(A,cache_dir):B=cache_dir;A.cache_dir=B;A.cache_dir.mkdir(parents=_A,exist_ok=_A);A.cache_file=B/'license_cache.json'
	def get(B,key):
		'Get cached verification result if not expired';A=key
		if not B.cache_file.exists():return
		try:
			with open(B.cache_file,'r')as E:C=json.load(E)
			if A not in C:return
			D=C[A];F=D.get(_D,0)
			if time.time()<F:log.debug(f"Cache hit for {A}");return D.get('data')
			else:log.debug(f"Cache expired for {A}");return
		except Exception as G:log.warning(f"Failed to read cache: {G}");return
	def set(A,key,data,ttl=3600):
		'Cache verification result with TTL (default 1 hour)'
		try:
			B={}
			if A.cache_file.exists():
				with open(A.cache_file,'r')as C:B=json.load(C)
			B[key]={'data':data,_D:time.time()+ttl,'cached_at':time.time()}
			with open(A.cache_file,'w')as C:json.dump(B,C,indent=2)
			log.debug(f"Cached verification for {key} (TTL: {ttl}s)")
		except Exception as D:log.warning(f"Failed to write cache: {D}")
	def clear(A):
		'Clear all cached data'
		if A.cache_file.exists():A.cache_file.unlink();log.info('License cache cleared')
class LicenseVerifier:
	"\n    Real-time license verifier for vnstock packages.\n    \n    Verifies user's license status before allowing package usage.\n    Caches results to minimize API calls.\n    "
	def __init__(A,project_dir,api_base_url='https://vnstocks.com',cache_ttl=3600):'\n        Initialize license verifier.\n        \n        Args:\n            project_dir: Directory containing api_key.json\n            api_base_url: Base URL for vnstock API\n            cache_ttl: Cache time-to-live in seconds (default: 1 hour)\n        ';B=project_dir;A.project_dir=B;A.api_base_url=api_base_url.rstrip('/');A.cache_ttl=cache_ttl;A.api_key_path=B/'api_key.json';A.cache=LicenseCache(B/'.cache');A.session=requests.Session()
	def get_api_key(A):
		'Get API key from file or environment';B=os.getenv('VNSTOCK_API_KEY')
		if B:return B
		if A.api_key_path.exists():
			try:
				with open(A.api_key_path,'r')as C:D=json.load(C);return D.get(_C)
			except Exception as E:log.warning(f"Failed to read API key: {E}")
		else:log.debug(f"api_key.json not found at: {A.api_key_path}")
	def get_device_id(C):
		'Get device ID'
		try:from.device import get_unified_device_id as A;return A()
		except Exception as B:log.error(f"Failed to get device ID: {B}");return'unknown'
	def verify_license(B,package_name,version='1.0.0',force=_B):
		'\n        Verify license for a specific package.\n        \n        Args:\n            package_name: Name of package to verify\n            version: Package version (default: "1.0.0")\n            force: Force verification, bypass cache\n        \n        Returns:\n            True if license is valid, False otherwise\n            \n        Raises:\n            SystemExit on critical license failures\n        ';O='error';N='success';H=version;C=package_name;G=f"{C}_{H}"
		if not force:
			E=B.cache.get(G)
			if E and E.get(N):log.debug(f"Using cached license verification for {C}");return _A
		I=B.get_api_key()
		if not I:
			from.utils import is_google_colab as P;J=B.api_key_path;A=f"""❌ License Error: No API key found
   Expected location: {J}

Please either:
1. Set VNSTOCK_API_KEY environment variable, or
2. Create api_key.json file at: {J}
"""
			if P():A+='\n💡 For Google Colab:\n   Your api_key.json should be in Google Drive:\n   /content/drive/MyDrive/.vnstock/api_key.json\n   \n   Make sure Google Drive is mounted first!'
			raise SystemExit(A)
		Q=B.get_device_id()
		try:
			log.debug(f"Verifying license for {C}...");R={_C:I,_E:Q,'package_name':C,'version':H};S=f"{B.api_base_url}/api/vnstock/license/verify";D=B.session.post(S,json=R,timeout=10)
			if D.status_code==200:
				F=D.json()
				if F.get(N):B.cache.set(G,F,B.cache_ttl);T=F.get('subscription',{}).get('tier','N/A');log.debug(f"License verified for {C}: {T}");return _A
				else:U=F.get(O,'Unknown error');log.error(f"License verification failed: {U}");return _B
			elif D.status_code==403:
				K=D.json();V=K.get(O,'Access denied');L=K.get('action','');A=f"❌ License Error: {V}\n   Package: {C}\n"
				if L:A+=f"   Action: {L}\n"
				A+='\nPlease check your subscription at https://vnstocks.com/account';raise SystemExit(A)
			elif D.status_code==401:A='❌ License Error: Invalid API key\nPlease check your credentials and try again.';raise SystemExit(A)
			else:log.error(f"License verification failed: HTTP {D.status_code}");return _B
		except requests.exceptions.RequestException as M:
			log.warning(f"Network error during verification: {M}");E=B.cache.get(G)
			if E:log.info('Using expired cache due to network error (offline mode)');return _A
			A=f"❌ License Error: Cannot verify license for {C}\nNetwork error: {M}\n\nPlease check your internet connection and try again.";raise SystemExit(A)
	def verify_all_packages(C,packages,force=_B):
		'\n        Verify licenses for multiple packages.\n        \n        Args:\n            packages: List of package names\n            force: Force verification, bypass cache\n        \n        Returns:\n            Dict mapping package name to verification status\n        ';A={}
		for B in packages:
			try:A[B]=C.verify_license(B,force=force)
			except SystemExit:A[B]=_B
		return A
	def get_license_info(A):
		'\n        Get current license information without verification.\n        \n        Returns:\n            License info dict or None\n        ';B=A.get_api_key()
		if not B:return
		D=A.get_device_id()
		try:
			E=f"{A.api_base_url}/api/vnstock/license/verify";F={_C:B,_E:D};C=A.session.get(E,params=F,timeout=10)
			if C.status_code==200:return C.json()
		except Exception as G:log.debug(f"Failed to get license info: {G}")
def verify_package_license(package_name,project_dir=None):
	'\n    Convenience function to verify license for a package.\n    \n    Args:\n        package_name: Name of package to verify\n        project_dir: Optional project directory (auto-detected if None)\n    \n    Returns:\n        True if license is valid\n        \n    Raises:\n        SystemExit on license failure\n    ';A=project_dir
	if A is None:from.utils import get_vnstock_directory as B;A=B()
	C=LicenseVerifier(A);return C.verify_license(package_name)