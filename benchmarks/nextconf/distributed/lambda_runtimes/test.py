import sys
import os
import ctypes
import select

libc = ctypes.CDLL("libc.so.6")
print("Hello from python")

sys.stdout.write(' \b')
pipe_out, pipe_in = os.pipe()
stdout = os.dup(1)
os.dup2(pipe_in, 1)
# check if we have more to read from the pipe
def more_data():
        r, _, _ = select.select([pipe_out], [], [], 0)
        return bool(r)

# read the whole pipe
def read_pipe():
        out = ''
        while more_data():
                out += os.read(pipe_out, 1024)

        return out

libc.printf("Hello from libc!")
os.dup2(stdout, 1)
print("DONE WITH printf")
print(read_pipe())
print("^^")
