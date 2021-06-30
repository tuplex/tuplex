# (c) 2021 Tuplex contributors
# a ready-to-run Tuplex version on a jupyter image

FROM jupyter/minimal-notebook
LABEL maintainer="Tuplex team <tuplex@cs.brown.edu>"

USER root

RUN python3 -m pip install tuplex

# when using test.pypi.org
# RUN python3 -m pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple tuplex

USER ${NB_UID}

RUN rm -rf /home/${NB_USER}/work

# add HelloTuplex.ipynb file
ADD HelloTuplex.ipynb /home/${NB_USER}/HelloTuplex.ipynb
