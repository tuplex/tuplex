# (c) 2021 Tuplex contributors
# a ready-to-run Tuplex version on a jupyter image
# docker pull jupyter/minimal-notebook:python-3.9.13

# the tags and everything are broken. python-3.9.13 actually contains python3.10.6, but for whatever reason
# python-3.9.12 contains python 3.9.13 (??.??). Just use this for now -.-

FROM jupyter/minimal-notebook:python-3.9.12
LABEL maintainer="Tuplex team <tuplex@cs.brown.edu>"

USER root

# add HelloTuplex.ipynb file
ADD 00_HelloTuplex.ipynb /home/${NB_USER}/00_HelloTuplex.ipynb
ADD 01_Intro_to_Tuplex.ipynb /home/${NB_USER}/01_Intro_to_Tuplex.ipynb
ADD 02_Working_with_files.ipynb /home/${NB_USER}/02_Working_with_files.ipynb
ADD sample_data/california_housing_train.csv /home/${NB_USER}/sample_data/california_housing_train.csv

# chown everything to ${NB_USER}:users
RUN chown -R ${NB_USER}:users /home/${NB_USER}

# use notebook environment
ENV DOCKER_STACKS_JUPYTER_CMD=notebook

USER ${NB_UID}
ENV PATH=/home/${NB_USER}/.local/bin:$PATH

RUN rm -rf /home/${NB_USER}/work

RUN python3 -m pip install tuplex gdown --user

# when using test.pypi.org
# RUN python3 -m pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple tuplex
