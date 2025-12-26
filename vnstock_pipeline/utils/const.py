import pathlib
HOME_DIR=pathlib.Path.home()
PROJECT_DIR=HOME_DIR/'.vnstock'
ID_DIR=PROJECT_DIR/'id'
PACKAGE_MAPPING={'browser':['vnstock3','vnstock','vnstock_data','vnstock_ta','vnstock_pipeline'],'charting':['vnstock_ezchart']}