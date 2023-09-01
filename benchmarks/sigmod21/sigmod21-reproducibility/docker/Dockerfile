FROM python:3.9.10-bullseye

RUN apt-get update && apt-get install -y git

# RUN apt-get install -y texlive-full
# instead of pulling in full texlive, use just needed packages...
RUN apt-get install -y dvipng texlive-latex-extra texlive-fonts-recommended cm-super

WORKDIR /work
ADD requirements.txt /work/requirements.txt
RUN python3.9 -m pip install -r /work/requirements.txt

RUN git clone https://github.com/LeonhardFS/tuplex-public.git /work/tuplex && cd /work/tuplex && git checkout --track origin/sigmod-repro && mkdir -p /scripts && cp -r benchmarks/sigmod21-reproducibility/*.py /scripts && cp -r benchmarks/sigmod21-reproducibility/plot_scripts /scripts/plot_scripts