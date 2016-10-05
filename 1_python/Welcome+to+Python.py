
# coding: utf-8

# <div class="clearfix" style="padding: 10px; padding-left: 0px">
# <img src="https://raw.githubusercontent.com/jupyter/nature-demo/master/images/jupyter-logo.png" width="150px" style="display: inline-block; margin-top: 5px;">
# <a href="http://bit.ly/tmpnbdevrax"><img src="https://cloud.githubusercontent.com/assets/836375/4916141/2732892e-64d6-11e4-980f-11afcf03ca31.png" width="150px" class="pull-right" style="display: inline-block; margin: 0px;"></a>
# </div>
# 
# ## Welcome to the Temporary Notebook (tmpnb) service!
# 
# This Notebook Server was **launched just for you**. It's a temporary way for you to try out a recent development version of the IPython/Jupyter notebook.
# 
# <div class="alert alert-warning" role="alert" style="margin: 10px">
# <p>**WARNING**</p>
# 
# <p>Don't rely on this server for anything you want to last - your server will be *deleted after 10 minutes of inactivity*.</p>
# </div>
# 
# Your server is hosted thanks to [Rackspace](http://bit.ly/tmpnbdevrax), on their on-demand bare metal servers, [OnMetal](http://bit.ly/onmetal).
# 

# ### Run some Python code!
# 
# To run the code below:
# 
# 1. Click on the cell to select it.
# 2. Press `SHIFT+ENTER` on your keyboard or press the play button (<button class='fa fa-play icon-play btn btn-xs btn-default'></button>) in the toolbar above.
# 
# A full tutorial for using the notebook interface is available [here](ipython_examples/Notebook/Index.ipynb).

# In[ ]:

get_ipython().magic('matplotlib notebook')

import pandas as pd
import numpy as np
import matplotlib

from matplotlib import pyplot as plt
import seaborn as sns

ts = pd.Series(np.random.randn(1000), index=pd.date_range('1/1/2000', periods=1000))
ts = ts.cumsum()

df = pd.DataFrame(np.random.randn(1000, 4), index=ts.index,
                  columns=['A', 'B', 'C', 'D'])
df = df.cumsum()
df.plot(); plt.legend(loc='best')


# Feel free to open new cells using the plus button (<button class='fa fa-plus icon-plus btn btn-xs btn-default'></button>), or hitting shift-enter while this cell is selected.
# 
# Behind the scenes, the software that powers this is [tmpnb](https://github.com/jupyter/tmpnb), a  Tornado application that spawns [pre-built Docker containers](https://github.com/ipython/docker-notebook) and then uses the [jupyter/configurable-http-proxy](https://github.com/jupyter/configurable-http-proxy) to put your notebook server on a unique path.
