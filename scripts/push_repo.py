import os
import subprocess

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout)
    if res.stderr:
        print(res.stderr)
    return res.returncode

def main():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("Error: No GITHUB_TOKEN or GH_TOKEN found.")
        return
    
    # Remove existing remote if already added
    subprocess.run("git remote remove origin", shell=True, capture_output=True)
    
    # Add remote with token
    remote_with_token = f"https://Anshu666666:{token}@github.com/Anshu666666/TransactFlow.git"
    run_cmd(f'git remote add origin "{remote_with_token}"')
    
    print("Pushing to GitHub (main)...")
    code = run_cmd("git push -u origin main")
    
    # Sanitize remote URL so token is never stored on disk in .git/config
    clean_remote = "https://github.com/Anshu666666/TransactFlow.git"
    run_cmd(f'git remote set-url origin "{clean_remote}"')
    
    if code == 0:
        print("SUCCESS: Pushed to https://github.com/Anshu666666/TransactFlow")
    else:
        print(f"FAILED: git push exited with code {code}")

if __name__ == "__main__":
    main()
