#!/usr/bin/env python3
"""
Daily Scheduler Setup
======================
Sets up automatic daily execution of the NSE screener pipeline.

For macOS: Creates a launchd plist that runs after market close (18:30 IST).
For Linux: Creates a crontab entry.

Usage:
  python scripts/setup_scheduler.py --install
  python scripts/setup_scheduler.py --uninstall
  python scripts/setup_scheduler.py --status
"""
from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
PLIST_NAME = "com.nse-screener.daily-run"
PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / f"{PLIST_NAME}.plist"

CRON_COMMENT = "# NSE Screener daily run"


def get_plist_content() -> str:
    """Generate launchd plist XML for macOS scheduling."""
    make_path = "/usr/bin/make"
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_NAME}</string>

    <key>ProgramArguments</key>
    <array>
        <string>{make_path}</string>
        <string>-C</string>
        <string>{PROJECT_ROOT}</string>
        <string>daily-run</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>19</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>WorkingDirectory</key>
    <string>{PROJECT_ROOT}</string>

    <key>StandardOutPath</key>
    <string>{log_dir}/daily_run_stdout.log</string>

    <key>StandardErrorPath</key>
    <string>{log_dir}/daily_run_stderr.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:{PROJECT_ROOT}/.venv/bin</string>
        <key>HOME</key>
        <string>{Path.home()}</string>
    </dict>

    <key>RunAtLoad</key>
    <false/>

    <key>Nice</key>
    <integer>10</integer>
</dict>
</plist>
"""


def get_cron_entry() -> str:
    """Generate crontab entry for Linux scheduling."""
    return f"0 19 * * 1-5 cd {PROJECT_ROOT} && make daily-run >> {PROJECT_ROOT}/logs/daily_run.log 2>&1"


def install_macos():
    """Install launchd plist on macOS."""
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    PLIST_PATH.write_text(get_plist_content())
    print(f"Created: {PLIST_PATH}")

    # Load the plist
    result = subprocess.run(
        ["launchctl", "load", str(PLIST_PATH)],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print(f"Loaded: {PLIST_NAME}")
        print("Daily run scheduled at 19:00 (7 PM) local time, Mon-Fri")
    else:
        print(f"Warning: Could not load plist: {result.stderr}")
        print(f"Try manually: launchctl load {PLIST_PATH}")


def uninstall_macos():
    """Uninstall launchd plist on macOS."""
    if PLIST_PATH.exists():
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)], capture_output=True)
        PLIST_PATH.unlink()
        print(f"Removed: {PLIST_PATH}")
    else:
        print("No plist found to remove")


def status_macos():
    """Check launchd status on macOS."""
    result = subprocess.run(
        ["launchctl", "list"],
        capture_output=True, text=True,
    )
    for line in result.stdout.splitlines():
        if PLIST_NAME in line:
            print(f"Status: {line}")
            return
    print(f"Not loaded: {PLIST_NAME}")
    if PLIST_PATH.exists():
        print(f"Plist exists at {PLIST_PATH} but is not loaded")
    else:
        print("Plist not installed")


def install_linux():
    """Install crontab entry on Linux."""
    entry = get_cron_entry()

    # Get existing crontab
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = result.stdout if result.returncode == 0 else ""

    if "nse-screener" in existing.lower() or "daily-run" in existing:
        print("Cron entry already exists")
        return

    new_cron = existing.rstrip() + f"\n{CRON_COMMENT}\n{entry}\n"
    proc = subprocess.run(
        ["crontab", "-"],
        input=new_cron, capture_output=True, text=True,
    )
    if proc.returncode == 0:
        print("Cron entry installed")
        print(f"Scheduled: {entry}")
    else:
        print(f"Error: {proc.stderr}")


def uninstall_linux():
    """Remove crontab entry on Linux."""
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        print("No crontab found")
        return

    lines = result.stdout.splitlines()
    new_lines = [l for l in lines if "nse-screener" not in l.lower() and "daily-run" not in l]
    new_cron = "\n".join(new_lines) + "\n"

    subprocess.run(["crontab", "-"], input=new_cron, capture_output=True, text=True)
    print("Cron entry removed")


def parse_args():
    parser = argparse.ArgumentParser(description="Setup daily scheduling")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--install", action="store_true", help="Install scheduler")
    group.add_argument("--uninstall", action="store_true", help="Remove scheduler")
    group.add_argument("--status", action="store_true", help="Check scheduler status")
    group.add_argument("--show", action="store_true", help="Show config without installing")
    return parser.parse_args()


def main():
    args = parse_args()
    is_mac = platform.system() == "Darwin"

    if args.show:
        if is_mac:
            print("=== launchd plist ===")
            print(get_plist_content())
        else:
            print("=== crontab entry ===")
            print(get_cron_entry())
        return

    if args.install:
        if is_mac:
            install_macos()
        else:
            install_linux()
    elif args.uninstall:
        if is_mac:
            uninstall_macos()
        else:
            uninstall_linux()
    elif args.status:
        if is_mac:
            status_macos()
        else:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            for line in result.stdout.splitlines():
                if "nse-screener" in line.lower() or "daily-run" in line:
                    print(f"Active: {line}")
                    return
            print("No cron entry found")


if __name__ == "__main__":
    main()
