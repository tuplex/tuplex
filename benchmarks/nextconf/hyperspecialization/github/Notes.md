## JSON libraries in C

cJSON seems to be a rather slow JSON library written in C. There are multiple alternatives out there:

https://github.com/ibireme/yyjson is the basis of https://github.com/ijl/orjson, and claims to be faster than simdjson.

https://github.com/ultrajson/ultrajson seems slower than yyjson, but faster than cJSON and has a conversion function to PyObjects which may be super useful.