def check_required_columns(data,required_columns:list)->bool:
	A=data
	if not isinstance(A,dict):A=A.__dict__
	B=[B for B in required_columns if B not in A];return len(B)==0