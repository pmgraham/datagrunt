#!/usr/bin/env python3
"""Antigravity workflow script to run the AI agent that updates the doc site and drafts the release blog post."""

import argparse
import asyncio
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Try importing dotenv
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def run_command(cmd, cwd=None):
    """Run a shell command and return its output and exit code."""
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, cwd=cwd)
    return res.returncode, res.stdout.strip(), res.stderr.strip()


def get_version(repo_dir):
    """Read the current version of the project from pyproject.toml."""
    # A simple parser for pyproject.toml version
    try:
        pyproject_path = repo_dir / "pyproject.toml"
        if pyproject_path.exists():
            with open(pyproject_path, "r") as f:
                for line in f:
                    if line.strip().startswith("version ="):
                        # Extract the string, e.g., version = "3.1.6"
                        val = line.split("=")[1].strip().strip('"').strip("'")
                        return val
    except Exception as e:
        print(f"[!] Warning: Failed to parse version from pyproject.toml: {e}")
    return "unknown"


async def run_agent(version, commit_log, file_stat, git_diff, repo_dir):
    """Run the Antigravity Agent to update docs and write the blog post."""
    from google.antigravity import Agent, LocalAgentConfig
    from google.antigravity.hooks import policy

    # Define directories
    save_dir = str(repo_dir / ".antigravity_sessions")
    app_data_dir = str(repo_dir / ".antigravity_brain")

    # Ensure directories exist
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(app_data_dir, exist_ok=True)

    # Configure the system instructions
    system_instructions = (
        "You are a technical writer and release coordination agent for Datagrunt.\n"
        f"The current local date is: {datetime.now().strftime('%Y-%m-%d')}.\n\n"
        "Your goal is to:\n"
        "1. Analyze the git diff, file statistics, and commit log of the datagrunt codebase (located at `/Users/pmgraham/projects/datagrunt`).\n"
        "2. Identify new features, bug fixes, performance improvements, or breaking changes.\n"
        "3. Update the documentation in the Hugo site located at `/Users/pmgraham/projects/datagrunt-site/content/docs/_index.md` or other files. You must read the existing documentation first if you want to update it accurately. Ensure you format links and structures using Markdown.\n"
        "4. Write a release blog post under `/Users/pmgraham/projects/datagrunt-site/content/blog/2026/june/` (or current year/month folder as appropriate). Use the format `datagrunt-<version>-<short-description>.md`.\n"
        "   - The blog post must have front matter at the top: \n"
        "     ---\n"
        "     title: \"Datagrunt <version>: <engaging headline>\"\n"
        "     date: <YYYY-MM-DD>\n"
        "     draft: true\n"
        "     author: \"Martin Graham\"\n"
        "     author_email: \"datagrunt@datagrunt.io\"\n"
        "     description: \"<engaging description of release highlights>\"\n"
        "     tags: [\"python\", \"csv\", \"data-engineering\", \"release-notes\"]\n"
        "     ---\n"
        "   - The post should have a structured, premium technical style with code snippets explaining how to use new features.\n"
        "5. Output a summary of the files updated and created (including their absolute paths) and a brief overview of the content generated.\n"
        "6. Do not mark the blog post draft: false yet. The user will review and approve it."
    )

    # Check if using Vertex AI
    use_vertex = os.getenv("USE_VERTEX", "").lower() in ("true", "1", "yes")
    gcp_project = os.getenv("GCP_PROJECT")
    gcp_location = os.getenv("GCP_LOCATION", "us-central1")

    config_kwargs = {
        "model": "gemini-3.5-flash",
        "system_instructions": system_instructions,
        "app_data_dir": app_data_dir,
        "save_dir": save_dir,
        # Allow all tools so we can read and write files in both repos and run git log
        "policies": [policy.allow_all()],
    }
    if use_vertex:
        config_kwargs["vertex"] = True
        if gcp_project:
            config_kwargs["project"] = gcp_project
        if gcp_location:
            config_kwargs["location"] = gcp_location

    config = LocalAgentConfig(**config_kwargs)

    prompt = f"""
Please process the release changes for Datagrunt version {version}.

--- COMMIT LOG ---
{commit_log}

--- FILE STATISTICS ---
{file_stat}

--- FULL GIT DIFF ---
{git_diff[:25000]}  # Limit diff size if it is extremely long
"""

    print("[*] Starting Antigravity Agent...")
    async with Agent(config) as agent:
        response = await agent.chat(prompt)
        async for token in response:
            print(token, end="", flush=True)
        print()

        # Save conversation ID
        conv_id = agent.conversation_id
        session_file = repo_dir / ".antigravity_latest_session"
        with open(session_file, "w") as f:
            f.write(conv_id)

        print("\n================================================================================")
        print("[+] Antigravity session saved.")
        print(f"[*] Conversation ID: {conv_id}")
        print("To review the blog post locally and guide updates, run:")
        print("  python scripts/review_blog.py")
        print("================================================================================")


def main():
    parser = argparse.ArgumentParser(
        description="Run Antigravity Agent to write docs and blog post draft based on git changes."
    )
    parser.add_argument(
        "--pre-merge-commit",
        default="HEAD~1",
        help="The commit/ref before the merge. Used to compute the diff. Defaults to HEAD~1.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the script (analyze git but do not call the Agent).",
    )
    args = parser.parse_args()

    repo_dir = Path(__file__).parent.parent.resolve()

    # Load version
    version = get_version(repo_dir)
    print(f"[*] Project Version: {version}")

    # Gather git log, stats, and diff
    print(f"[*] Gathering git changes between {args.pre_merge_commit} and HEAD...")
    code_log, commit_log, _ = run_command(
        f"git log {args.pre_merge_commit}..HEAD --oneline --no-merges", cwd=repo_dir
    )
    code_stat, file_stat, _ = run_command(f"git diff --stat {args.pre_merge_commit} HEAD", cwd=repo_dir)
    code_diff, git_diff, _ = run_command(f"git diff {args.pre_merge_commit} HEAD", cwd=repo_dir)

    if code_log != 0 or code_diff != 0:
        print(f"[!] Warning: Git commands failed. Check if '{args.pre_merge_commit}' is a valid commit.")
        print("[*] Falling back to analyzing the latest commit (HEAD~1..HEAD)...")
        args.pre_merge_commit = "HEAD~1"
        _, commit_log, _ = run_command("git log -n 5 --oneline --no-merges", cwd=repo_dir)
        _, file_stat, _ = run_command("git diff --stat HEAD~1 HEAD", cwd=repo_dir)
        _, git_diff, _ = run_command("git diff HEAD~1 HEAD", cwd=repo_dir)

    print(f"[*] Commits found:\n{commit_log}\n")
    print(f"[*] Files changed:\n{file_stat}\n")

    if args.dry_run:
        print("[Dry-Run] Git changes analyzed. Exiting dry run mode.")
        sys.exit(0)

    # Check for GEMINI_API_KEY or Vertex AI configurations
    use_vertex = os.getenv("USE_VERTEX", "").lower() in ("true", "1", "yes")
    if not use_vertex and not os.getenv("GEMINI_API_KEY"):
        print("[!] Error: No authentication configured.")
        print("[!] Please configure one of the following in your .env file:")
        print("    1. Google AI Studio (Free Tier): set GEMINI_API_KEY")
        print("    2. Vertex AI (GCP): set USE_VERTEX=True and GCP_PROJECT=your-project-id")
        sys.exit(1)

    # Run the async agent
    asyncio.run(
        run_agent(
            version=version,
            commit_log=commit_log,
            file_stat=file_stat,
            git_diff=git_diff,
            repo_dir=repo_dir,
        )
    )


if __name__ == "__main__":
    main()
