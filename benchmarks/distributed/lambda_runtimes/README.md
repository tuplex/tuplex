Pull `python_resources` from `s3://tuplex-test-rahuly/python_resources.zip` (or copy from `../../../tuplex/awslambda/python38_resources.zip`)

In each folder, run `init.sh` (if it exists) to initialize the package, and then run `update.sh` to upload to Lambda

To create `python_resources`:
 - Create Amazon Linux 2 EC2 instance
 - `bin`: `/usr/bin/python3`
 - `lib`
   - `/usr/lib64/libpython3.so`
   - `/usr/lib64/libpython3.7m.so`
   - `/usr/lib64/libpython3.7m.so.1.0`
   - `/usr/lib/python3.7`
 - `usr_lib`: `/usr/local/lib/python3.7`
 - `lib64`: `/usr/lib64/python3.7`
 - `include`: `/usr/include/python3.7m/`
