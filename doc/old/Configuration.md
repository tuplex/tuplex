## Tuplex configuration options
There are various ways on how you can customize the Tuplex framework. Every context has the option to configure various components.
Configuration options can either be passed via a config file or directly when creating a context object. When no configuration is provided, Tuplex uses defaults.

### Configuration options
|key|default|description|
|---|---|---|
|`tuplex.allowUndefinedBehavior`|`false`| When setting this value to true, the Python UDF compiler won't add checks for operators. I.e. there is no security for division by zero etc. This may lead to faster code but may yield errors.|
|`tuplex.autoUpcast`|`false`|When passing a mixture booleans, integers or floating types within a column, this setting controls whether the framework should automatically upcast to the larger type. If true, when booleans and integers are encountered, the type will be integer. Similarly booleans and floats will result in floats and a mixture of booleans, integers and floating point values will result in float values. May introduce rounding/floating point errors.|
|`tuplex.executorMemory`|`1GB`|Amount of memory to use for computation. This is a hard limit. Tuplex will use a LRU strategy to evict the least recently used partition to memory if this fills up.|
|`tuplex.logDir`|`.`|Directory where to store the log for a job|
|`tuplex.partitionSize`|`1MB`|Size of a partition. If a single row of data doesn't fit in, Tuplex will automatically resize the partition. This will also determine the parallelism.|
|`tuplex.runTimeMemory`|`32MB`|UDFs may allocate memory, e.g. when string operations are involved. This controls how much memory is initially allocated for UDF operations. This is a soft limit. If UDFs require more memory, it will be allocated and remain for the rest of the computation until a stage is completed.|
|`tuplex.runTimeMemoryBlockSize`|`4MB`|Runtime memory is allocated in blocks. Growth above the limit `tuplex.runTimeMemory` provides will be done in blocks of size `tuplex.runTimeMemoryBlockSize`.|
|`tuplex.scratchDir`|`/tmp`|Location where tuplex will store temporary files, partitions spilled out to disk etc.|
|`tuplex.useLLVMOptimizer`|`true`|Configure whether generated code should be optimized using LLVM Optimization passes or not.|

Tuplex will look for configuration in the following way:
1. If the environment variable `FASTETL_HOME` is set and a file `config.yaml` exists there, Tuplex will load its configuration keys.
2. If in the current directory (`.`) a file `config.yaml` exists, Tuplex will load it and overwrite configuration options obtained via 1.
3. If the user specifies options upon creation of the context object, these settings will overwrite any configuration settings obtained via yaml files. 
Also a passed yaml file via the context object will overwrite existing settings.