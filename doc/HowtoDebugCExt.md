## How to debug using python

for this to work, we need a debug build of the cpython interpreter.

**Step 1:** download a compatible python version, e.g.
```bash
curl -LO https://github.com/python/cpython/archive/v3.9.2.zip
```
**Step 2:** unpack archive
```bash
tar xf v3.9.2 && mv cpython-3.9.2/ python3-debug && cd python3-debug
```

**Step 3:** build cpython in debug mode

For a complete list of options, look under <https://pythonextensionpatterns.readthedocs.io/en/latest/debugging/debug_python.html>
Yet, we use the following to create a valgrind compatible debug build:
We'll install the debug build to `/opt/python3.9-debug`
```bash
mkdir debug
cd debug
../configure --prefix=/opt/python3.9-debug --with-pydebug --without-pymalloc
make
make test
make install
```
Note that on Mac OS X, you'll have to add `--with-openssl=$(brew --prefix openssl)`. The detailed python guide can be found here: https://devguide.python.org/setup/
To build tuplex, you'll need to install the cloudpickle module. This should be done for the new python3 debug version

For Mac OS X:
```asm
../configure --prefix=/opt/python3.9-debug --with-pydebug --without-pymalloc --with-openssl=$(brew --prefix openssl) --enable-shared
```

TO work with the stuff, do `sudo chown -R $(whoami):admin /opt/python3.9-debug/lib/python3.9/site-packages/`

**Step 4:** build Tuplex against alternative python3 root. I.e., specify -DPython3_ROOT_DIR=<python3-debug/debug>

```bash
make -DPython3_ROOT_DIR=/opt/python3.9-debug -DCMAKE_BUILD_TYPE=Debug ..
```

To debug then a bad file, use

```asm
python3 -q -X faulthandler test.py
```



### Using Valgrind
https://pythonextensionpatterns.readthedocs.io/en/latest/debugging/valgrind.html#using-valgrind-label


Links:

- Adding trace ref support to find culprit: https://www.oreilly.com/library/view/python-cookbook/0596001673/ch16s09.html