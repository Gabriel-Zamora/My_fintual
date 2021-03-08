import os
import sys; sys.path.append(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/fintual/fun")
from save import * 

psql_start(auth, BBDD)
save_assets()    
save_funds()
save_series()
save_my_goals()
save_my_fintual()