#!/usr/bin/env bash
# config_tmux.sh — Install tmux on a worker node that lacks it.
# Called by cc_bridge.py (or manually) when tmux is not found.
#
# Strategy:
#   1. Check if tmux already exists → done
#   2. Try apt-get install (internal mirror first, then default)
#   3. Fallback: download static AppImage binary via proxy
#
# Usage: bash config_tmux.sh
# Exit 0 on success, 1 on failure.

set -euo pipefail

PROXY="http://cmcproxy:WvUBhef4bQ@10.251.112.50:8128"
TMUX_APPIMAGE_URL="https://github.com/nelsonenzo/tmux-appimage/releases/download/3.3a/tmux.appimage"
INSTALL_PATH="/usr/local/bin/tmux"

# --- Already installed? ---
if command -v tmux &>/dev/null; then
    echo "tmux already installed: $(tmux -V)"
    exit 0
fi

echo "tmux not found, attempting install..."

# --- Method 1: apt-get with internal mirror ---
install_via_apt() {
    # Add baidu internal mirror if not present
    if [ ! -f /etc/apt/sources.list.d/baidu-mirror.list ]; then
        echo "deb http://mirrors.baidu.com/ubuntu/ focal main universe" > /etc/apt/sources.list.d/baidu-mirror.list
    fi
    apt-get update -qq 2>/dev/null
    apt-get install -y -qq tmux 2>/dev/null
}

# --- Method 2: Download static AppImage binary ---
install_via_appimage() {
    local tmp="/tmp/tmux.appimage"
    echo "Downloading tmux AppImage via proxy..."
    curl -sL -x "$PROXY" -o "$tmp" "$TMUX_APPIMAGE_URL"

    if [ ! -f "$tmp" ] || [ "$(stat -c%s "$tmp" 2>/dev/null || echo 0)" -lt 1000000 ]; then
        echo "Download failed or file too small"
        return 1
    fi

    # Check if it's directly executable (statically linked ELF)
    chmod +x "$tmp"
    if "$tmp" -V &>/dev/null; then
        cp "$tmp" "$INSTALL_PATH"
        chmod +x "$INSTALL_PATH"
        echo "Installed via AppImage (direct): $($INSTALL_PATH -V)"
        rm -f "$tmp"
        return 0
    fi

    # If it needs FUSE (AppImage format), extract it
    echo "Direct exec failed, trying --appimage-extract..."
    cd /tmp
    "$tmp" --appimage-extract &>/dev/null || true
    if [ -f /tmp/squashfs-root/usr/bin/tmux ]; then
        cp /tmp/squashfs-root/usr/bin/tmux "$INSTALL_PATH"
        chmod +x "$INSTALL_PATH"
        rm -rf /tmp/squashfs-root "$tmp"
        echo "Installed via AppImage (extracted): $($INSTALL_PATH -V)"
        return 0
    fi

    rm -f "$tmp"
    return 1
}

# --- Try methods in order ---
if install_via_apt 2>/dev/null; then
    echo "Installed via apt: $(tmux -V)"
    exit 0
fi

echo "apt failed, trying AppImage fallback..."

if install_via_appimage; then
    exit 0
fi

echo "ERROR: All install methods failed" >&2
exit 1
