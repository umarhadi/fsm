package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	poolMeta = "pool_meta"
	poolData = "pool_data"
	poolDev  = "/dev/mapper/pool"
)

func dmExists(name string) bool {
	// `dmsetup info <name>` exits 0 if mapping exists
	if err := exec.Command("dmsetup", "info", name).Run(); err == nil {
		return true
	}
	return false
}

func dmEnsureNode(name string) {
	// recreate device node if missing
	_ = exec.Command("dmsetup", "mknodes", name).Run()
}

// lookuporattachloop: find loop already attached to backing file; if none exists → attach new one
func lookupOrAttachLoop(backing string) (string, error) {
	out, _ := exec.Command("bash", "-lc", fmt.Sprintf(`losetup -j %q | awk -F: 'NR==1{print $1}'`, backing)).Output()
	dev := strings.TrimSpace(string(out))
	if dev != "" {
		return dev, nil
	}
	b, err := exec.Command("losetup", "-f", "--show", backing).Output()
	if err != nil {
		return "", fmt.Errorf("losetup --show %s: %w", backing, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// ensurethinpool: idempotent, reuses loop devices, and displays stderr when it fails
func ensureThinPool() error {
	// a) if "pool" mapping already exists in kernel → ensure node exists, then done
	if dmExists("pool") {
		dmEnsureNode("pool")
		return nil
	}
	// (checking node alone is not enough; don't use os.stat here)

	// b) prepare backing files
	if _, err := os.Stat(poolMeta); os.IsNotExist(err) {
		if err := exec.Command("fallocate", "-l", "1M", poolMeta).Run(); err != nil {
			return fmt.Errorf("fallocate pool_meta: %w", err)
		}
	}
	if _, err := os.Stat(poolData); os.IsNotExist(err) {
		if err := exec.Command("fallocate", "-l", "2G", poolData).Run(); err != nil {
			return fmt.Errorf("fallocate pool_data: %w", err)
		}
	}

	// c) reuse loop devices if already attached; if not, attach new ones
	meta, err := lookupOrAttachLoop(poolMeta)
	if err != nil {
		return err
	}
	data, err := lookupOrAttachLoop(poolData)
	if err != nil {
		return err
	}

	// d) create thin-pool; if it fails but mapping appears (race), consider it successful
	table := fmt.Sprintf("0 4194304 thin-pool %s %s 2048 32768", meta, data)

	var stderr bytes.Buffer
	cmd := exec.Command("dmsetup", "create", "--verifyudev", "pool", "--table", table)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// fallback: if mapping already exists (race / mknodes not yet executed)
		if dmExists("pool") {
			dmEnsureNode("pool")
			return nil
		}
		return fmt.Errorf("dmsetup create pool: %v (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}

	return nil
}

// createthin creates a thin logical volume with the given id.
// returns the device path (/dev/mapper/base_lv_{id}).
// idempotent: if device already exists, returns existing device.
func createThin(id int64) (string, error) {
	name := fmt.Sprintf("base_lv_%d", id)
	dev := filepath.Join("/dev/mapper", name)

	// check if device mapping already exists
	if dmExists(name) {
		dmEnsureNode(name)
		return dev, nil
	}

	// send message to create thin volume in pool (ignore error if already exists)
	_ = exec.Command("dmsetup", "message", poolDev, "0",
		fmt.Sprintf("create_thin %d", id)).Run()

	// create device mapping
	table := fmt.Sprintf("0 4194304 thin %s %d", poolDev, id)
	if err := exec.Command("dmsetup", "create", name, "--table", table).Run(); err != nil {
		// if create failed, check if it now exists (race condition)
		if dmExists(name) {
			dmEnsureNode(name)
			return dev, nil
		}
		return "", fmt.Errorf("dmsetup create %s: %w", name, err)
	}
	return dev, nil
}

// createsnap creates a snapshot of a base thin volume.
// idempotent: if snapshot already exists, returns existing device.
func createSnap(baseID, snapID int64) (string, error) {
	name := fmt.Sprintf("snap_lv_%d", snapID)
	dev := filepath.Join("/dev/mapper", name)

	// check if snapshot mapping already exists
	if dmExists(name) {
		dmEnsureNode(name)
		return dev, nil
	}

	// send message to create snapshot in pool (ignore error if already exists)
	_ = exec.Command("dmsetup", "message", poolDev, "0",
		fmt.Sprintf("create_snap %d %d", snapID, baseID)).Run()

	// create device mapping
	table := fmt.Sprintf("0 4194304 thin %s %d", poolDev, snapID)
	if err := exec.Command("dmsetup", "create", name, "--table", table).Run(); err != nil {
		// if create failed, check if it now exists (race condition)
		if dmExists(name) {
			dmEnsureNode(name)
			return dev, nil
		}
		return "", fmt.Errorf("dmsetup create %s: %w", name, err)
	}
	return dev, nil
}
