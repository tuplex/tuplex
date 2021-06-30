### PythonBindings
The core of the Tuplex pipeline is written in C++. In fact, it can be used without any Python.
 However, one of the primary goals is to provide users a Python frontend. 
 Glue code for this is stored in this component.
A pip installable package is contained within the tuplex subdirectory and 
a `setup.py` script within this directory. During the build phase, python files are copied 
to `dist/python` with the folder effectively containing then an installable pip package. 
In addition python unit tests from the directory tests are executed.
The shared object, generated using Boost.Python, is stored in `dist/python/libexec`.

### Testing Python bindings
To test the development version, go to `dist/python` and run 

```
python3 setup.py develop
```

Then, you can start the python interpreter and start running experiments. 
In order to execute the python unit tests, simply run
```
pytest
```
(be sure it is installed via pip3. Requires also the dev install upfront.

To uninstall the development version run
```
python3 setup.py develop --uninstall
```

To generate a source tar (i.e. source distribution) run
```
python3 setup.py sdist
```
The resulting tar.gz file can be found in the `dist/` folder.


(c) 2018 L.Spiegelberg