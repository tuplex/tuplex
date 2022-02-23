## README

If there are difficulties running the plot scripts, please use this docker file to plot everything. E.g., create
the container via

```
docker build -t sigmod21/plot .
```

Then, you can use the resulting container to run the plotting script.

Another option is to use the convenience script provided in this folder and plot e.g. the results out via

```
./plot-via-docker.sh /data/results
```

(Replace `/data/results` with a path to your results folder)

---