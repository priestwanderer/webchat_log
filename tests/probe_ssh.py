import subprocess
for command, data in [('printf ssh-ok',None),('python3 -','print("stdin-ok")\n')]:
    try:
        r=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','root@38.147.163.155',command],input=data,capture_output=True,text=True,timeout=12)
        print(command,r.returncode,r.stdout,r.stderr)
    except subprocess.TimeoutExpired:
        print(command,'STDIN_EOF_TIMEOUT')
