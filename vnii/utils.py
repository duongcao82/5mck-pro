'\nUtility functions for vnii\n'
_A='google.colab'
import sys
from pathlib import Path
def get_vnstock_directory():
	'\n    Get vnstock directory with proper Colab detection fallback.\n    \n    Priority:\n    1. Manual Colab detection first (most reliable)\n    2. vnstock.core.config.ggcolab.get_vnstock_directory() (if NOT Colab)\n    3. Local home directory\n    \n    Returns:\n        Path: vnstock directory path\n    ';C=_A in sys.modules
	if C:
		B=Path('/content/drive/MyDrive/.vnstock')
		if B.exists():return B
		elif Path('/content').exists():import logging as D;A=D.getLogger(__name__);A.warning('Google Drive not mounted or .vnstock not found');A.warning('Using temporary storage: /content/.vnstock');A.warning('Data will be lost when runtime restarts!');return Path('/content/.vnstock')
	try:from vnstock.core.config.ggcolab import get_vnstock_directory as E;return E()
	except ImportError:pass
	return Path.home()/'.vnstock'
def is_google_colab():
	'\n    Check if running in Google Colab.\n    \n    Returns:\n        bool: True if in Colab, False otherwise\n    '
	try:from vnstock.core.config.ggcolab import is_google_colab as A;return A()
	except ImportError:return _A in sys.modules