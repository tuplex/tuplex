# Running TPC-H with Hyper

To run hyper, need to install Tableau's hyper api <https://help.tableau.com/current/api/hyper_api/en-us/index.html> via

```
pip3 install tableauhyperapi
```

Run then via `nohup perflock bash benchmark.sh -hwloc &`