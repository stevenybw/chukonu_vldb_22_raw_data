# %%

import pandas as pd
import io
from IPython.display import display

TABLE = """baseline	nc	app	e2e	compute	appid
thrill	448	wc	12.0458	0
thrill	448	ts	258.418	0
thrill	448	pr	93.0919	0
thrill	448	cc	222.67	0
thrill	448	km	112.369	0
thrill	448	lr	72.804	0
husky	448	pr	109.696	0	
husky	448	km	72.6201	0	
husky	448	lr	161.957	0	
husky	448	wc	51.3479	0	
husky	448	cc	46.6031	0	
husky	448	ts	548.989	0	
1thread	1	pr		122	
1thread	1	km		12023.8	
1thread	1	lr			
1thread	1	wc		462.38	
1thread	1	cc		14.33	
1thread	1	ts		1864.48	
"""

def get():
    return pd.read_csv(io.StringIO(TABLE), sep='\t')

TABLE_COMPILE_TIME = """App	Spark	Chukonu	Thrill	Husky
WC	3.892	29.59	13.726	9.287
PR	4.851	52.46	19.971	10.66
KM	5.087	31.18	16.53	13.043
LR	5.328	24.13	19.539	11.549
CC	4.947	65.25	23.84	13.675
TS	4.723	26.96	14.493	10.05"""

def getCompileTime():
    return pd.read_csv(io.StringIO(TABLE_COMPILE_TIME), sep='\t')

TABLE_LINES_OF_CODE = """App	Spark	Chukonu	Thrill	Husky
WC	16	26	69	99
PR	36	43	147	111
KM	71	93	231	140
LR	46	75	145	186
CC	59	57	144	58
TS	18	17	52	139"""

def getLinesOfCode():
    return pd.read_csv(io.StringIO(TABLE_LINES_OF_CODE), sep='\t')

if __name__ == "__main__":
    # display(get())
    # display(getCompileTime())
    getCompileTime().set_index("App").plot.bar()

# %%
