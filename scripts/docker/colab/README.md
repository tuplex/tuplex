## Docker image to mimick Google's Colab environment
This folder contains a docker image mimicking (best-effort) the environment of Google's Colab.

To create the image, simply use
```
docker build -t tuplex/colab .
```

Then, start the container via
```
docker run -it tuplex/colab bash
```

---
(c) 2021 Tuplex team
