import os
import subprocess
import selectors
import uuid

def exec(cmd):
    return subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, executable='/bin/bash')

def runGetStdout(cmd):
    P = exec(cmd)
    return P.stdout.strip()

def runAndSaveResult(cmd, path):
    print(f"Run {cmd} and saves to {path}")
    P = exec(cmd)
    with open(path + ".stdout", "w") as out:
        out.write(P.stdout)
    with open(path + ".stderr", "w") as err:
        err.write(P.stderr)
    print(f"{cmd} terminated with error code {P.returncode} and saves to {path}")

def run(cmd):
    runAndSaveResult(cmd, f"/tmp/cmrcurrent{os.environ['USER']}-{uuid.uuid4().hex}")
