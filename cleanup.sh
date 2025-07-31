#!/bin/bash
# cleanup.sh - cleanup script for fsm orchestrator
# removes all devicemapper devices, unmounts volumes, detaches loop devices, and deletes backing files.

set -e

# check if running as root
if [ "$EUID" -ne 0 ]; then
  echo "ERROR: This script must be run as root (use sudo)"
  exit 1
fi

echo "=== Cleanup Script ==="
echo "This will remove all DeviceMapper devices, mounts, loop devices, and data files."
echo ""

# function to safely unmount with retries
safe_umount() {
  local mount_point=$1
  if mountpoint -q "$mount_point" 2>/dev/null; then
    echo "Unmounting: $mount_point"
    if ! umount "$mount_point" 2>/dev/null; then
      echo "  Retrying with lazy unmount..."
      umount -l "$mount_point" 2>/dev/null || echo "  Warning: Failed to unmount $mount_point"
    fi
  fi
}

# function to safely remove dmsetup device
safe_dmremove() {
  local device=$1
  if dmsetup info "$device" &>/dev/null; then
    echo "Removing device: $device"
    if ! dmsetup remove "$device" 2>/dev/null; then
      echo "  Retrying with force..."
      dmsetup remove --force "$device" 2>/dev/null || echo "  Warning: Failed to remove $device"
    fi
  fi
}

# 1. unmount all snapshot mounts
echo ""
echo "[1/8] Unmounting snapshot volumes..."
for mount in /mnt/images/*; do
  if [ -d "$mount" ]; then
    safe_umount "$mount"
  fi
done

# 2. unmount all base volume mounts
echo ""
echo "[2/8] Unmounting base volumes..."
for mount in /mnt/base_lv_*; do
  if [ -d "$mount" ]; then
    safe_umount "$mount"
  fi
done

# 3. remove all snapshot devices
echo ""
echo "[3/8] Removing snapshot devices..."
for device in $(dmsetup ls | grep "^snap_lv_" | awk '{print $1}'); do
  safe_dmremove "$device"
done

# 4. remove all base volume devices
echo ""
echo "[4/8] Removing base volume devices..."
for device in $(dmsetup ls | grep "^base_lv_" | awk '{print $1}'); do
  safe_dmremove "$device"
done

# 5. remove thin pool device
echo ""
echo "[5/8] Removing thin pool..."
safe_dmremove "pool"

# 6. detach loop devices
echo ""
echo "[6/8] Detaching loop devices..."
if [ -f "pool_meta" ]; then
  LOOP_META=$(losetup -j pool_meta | awk -F: '{print $1}')
  if [ -n "$LOOP_META" ]; then
    echo "Detaching loop device: $LOOP_META (pool_meta)"
    losetup -d "$LOOP_META" 2>/dev/null || echo "  Warning: Failed to detach $LOOP_META"
  fi
fi

if [ -f "pool_data" ]; then
  LOOP_DATA=$(losetup -j pool_data | awk -F: '{print $1}')
  if [ -n "$LOOP_DATA" ]; then
    echo "Detaching loop device: $LOOP_DATA (pool_data)"
    losetup -d "$LOOP_DATA" 2>/dev/null || echo "  Warning: Failed to detach $LOOP_DATA"
  fi
fi

# 7. delete backing files and directories
echo ""
echo "[7/8] Deleting backing files and directories..."

# delete devicemapper backing files
if [ -f "pool_meta" ]; then
  echo "Removing: pool_meta"
  rm -f pool_meta
fi

if [ -f "pool_data" ]; then
  echo "Removing: pool_data"
  rm -f pool_data
fi

# delete blob storage
if [ -d "blobs" ]; then
  echo "Removing: blobs/"
  rm -rf blobs/
fi

# delete fsm state database
if [ -d "fsmdb" ]; then
  echo "Removing: fsmdb/"
  rm -rf fsmdb/
fi

# delete sqlite database files
for dbfile in fsm.db fsm.db-shm fsm.db-wal; do
  if [ -f "$dbfile" ]; then
    echo "Removing: $dbfile"
    rm -f "$dbfile"
  fi
done

# delete results file
if [ -f "results.txt" ]; then
  echo "Removing: results.txt"
  rm -f results.txt
fi

# delete mount point directories
if [ -d "/mnt/images" ]; then
  echo "Removing: /mnt/images/"
  rm -rf /mnt/images/
fi

for dir in /mnt/base_lv_*; do
  if [ -d "$dir" ]; then
    echo "Removing: $dir"
    rm -rf "$dir"
  fi
done

# 8. verification
echo ""
echo "[8/8] Verifying cleanup..."

# check for remaining devicemapper devices
REMAINING_DM=$(dmsetup ls | grep -E "(pool|base_lv_|snap_lv_)" || true)
if [ -n "$REMAINING_DM" ]; then
  echo "WARNING: Some DeviceMapper devices still exist:"
  echo "$REMAINING_DM"
else
  echo "✓ All DeviceMapper devices removed"
fi

# check for remaining loop devices
REMAINING_LOOPS=$(losetup -j pool_meta 2>/dev/null || true; losetup -j pool_data 2>/dev/null || true)
if [ -n "$REMAINING_LOOPS" ]; then
  echo "WARNING: Some loop devices still attached:"
  echo "$REMAINING_LOOPS"
else
  echo "✓ All loop devices detached"
fi

# check for remaining mounts
REMAINING_MOUNTS=$(mount | grep -E "(/mnt/base_lv_|/mnt/images/)" || true)
if [ -n "$REMAINING_MOUNTS" ]; then
  echo "WARNING: Some mounts still active:"
  echo "$REMAINING_MOUNTS"
else
  echo "✓ All mounts removed"
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "Summary:"
echo "  - DeviceMapper devices removed"
echo "  - Volumes unmounted"
echo "  - Loop devices detached"
echo "  - Backing files deleted (pool_meta, pool_data)"
echo "  - Data directories removed (blobs/, fsmdb/)"
echo "  - Database files deleted (fsm.db)"
echo "  - Mount points cleaned up (/mnt/images/, /mnt/base_lv_*)"
echo ""
echo "you can now run './fsm' again for a fresh start."
