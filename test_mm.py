import subprocess
from subprocess import PIPE, Popen


# result = subprocess.run([
#     "metamap",
#     "-y",
#     "-JSONn",
#     "--silent",
# ], capture_output=True, check=True, shell=True, text=True, timeout=5, input="lung cancer")

# this is great and is the easiest working thing.
# it also requires no dependencies and can be made into a function/class taking the metamap binary path 
# (the folder technically so we can start servers) and the input text
# further we can just put the output directly and only need a switch for JSOn vs MMI

# TODO: test parallelism on this... it relies on WSD server so not sure if it will help...

input_command = Popen(["echo", "lung cancer"], stdout=PIPE)
mm_command = Popen(["metamap", "-N", "--silent"], stdin=input_command.stdout, stdout=PIPE)
output, err = mm_command.communicate()


for line in output.decode().splitlines()[1:]:
    print(line)

print("----")
# print(result.stderr)
