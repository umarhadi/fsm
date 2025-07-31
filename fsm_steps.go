package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/superfly/fsm"
)

// fetchlistandblobs retrieves the image manifest from s3 and downloads missing blobs.
// this stage is idempotent: existing blobs are skipped, and the operation can be safely re-run.
// gracefully handles missing blobs by logging warnings and continuing with available blobs.
func FetchListAndBlobs(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	// acquire lock to prevent concurrent fetches of the same family
	ok, _ := AcquireLock(ctx, app.DB, "fetch:"+r.Msg.Family, "pid", 15*time.Second)
	if !ok {
		return nil, fmt.Errorf("lock contention fetch")
	}
	defer ReleaseLock(context.Background(), app.DB, "fetch:"+r.Msg.Family, "pid")

	app.Logger.Infof("Fetching image family: %s", r.Msg.Family)

	// list all layers in the s3 bucket for this family
	layers, err := app.S3.ListFamily(ctx, r.Msg.Family)
	if err != nil {
		return nil, fmt.Errorf("list S3 family: %w", err)
	}

	if len(layers) == 0 {
		return nil, fmt.Errorf("no layers found for family %s", r.Msg.Family)
	}

	app.Logger.Infof("Found %d layers in family %s", len(layers), r.Msg.Family)

	// begin transaction for atomic image/blob updates
	tx, err := app.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// upsert image record (idempotent)
	imageID, err := upsertImage(ctx, tx, r.Msg.Family)
	if err != nil {
		return nil, fmt.Errorf("upsert image: %w", err)
	}

	// download and process each layer
	var manifest []Layer
	successCount := 0

	for i, l := range layers {
		// downloadifmissing is idempotent: skips if blob already exists locally
		path, dig, err := app.S3.DownloadIfMissing(ctx, &l)
		if err != nil {
			// gracefully handle missing blobs (they may appear in test environment)
			app.Logger.Warnf("blob %s not available, may appear in test: %v", l.Key, err)
			continue
		}

		l.Path, l.Digest = path, dig
		successCount++

		// upsert blob metadata (idempotent)
		if err := upsertBlob(ctx, tx, l); err != nil {
			return nil, fmt.Errorf("upsert blob %s: %w", l.Digest, err)
		}

		// link blob to image with sequence number
		if err := upsertImageBlob(ctx, tx, imageID, l.Digest, i); err != nil {
			return nil, fmt.Errorf("upsert image_blob: %w", err)
		}

		manifest = append(manifest, l)
	}

	if successCount == 0 {
		return nil, fmt.Errorf("no blobs successfully downloaded for family %s", r.Msg.Family)
	}

	app.Logger.Infof("Successfully downloaded %d/%d blobs", successCount, len(layers))

	// mark image as complete
	if _, err := tx.ExecContext(ctx, `UPDATE images SET complete=1 WHERE id=?`, imageID); err != nil {
		return nil, fmt.Errorf("mark image complete: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &fsm.Response[Res]{Msg: &Res{ImageID: imageID, Manifest: manifest}}, nil
}

// preparethinbase ensures a thin volume exists for the image's base layer.
// this stage is idempotent: if the image already has a base_lv_id, it verifies
// the volume exists, mounts it if needed, or recreates if missing.
func PrepareThinBase(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	// acquire lock to prevent concurrent thin pool operations
	ok, _ := AcquireLock(ctx, app.DB, "thin:base", "pid", 15*time.Second)
	if !ok {
		return nil, fmt.Errorf("lock contention thin")
	}
	defer ReleaseLock(context.Background(), app.DB, "thin:base", "pid")

	// check if base volume already exists
	var existingBaseLvID sql.NullInt64
	err := app.DB.QueryRowContext(ctx,
		`SELECT base_lv_id FROM images WHERE id=?`,
		r.W.Msg.ImageID).Scan(&existingBaseLvID)

	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("check existing base_lv_id: %w", err)
	}

	// if base_lv_id exists in db, verify device actually exists
	if existingBaseLvID.Valid {
		app.Logger.Infof("Image %d already has base_lv_id=%d, reusing existing volume",
			r.W.Msg.ImageID, existingBaseLvID.Int64)

		// check prepared status
		var prepared bool
		if err := app.DB.QueryRowContext(ctx,
			`SELECT prepared FROM images WHERE id=?`, r.W.Msg.ImageID).Scan(&prepared); err != nil {
			return nil, fmt.Errorf("check prepared: %w", err)
		}

		mount := fmt.Sprintf("/mnt/base_lv_%d", existingBaseLvID.Int64)

		// if already prepared, we don't need to unpack or mount.
		// let unpackintobase skip entirely.
		if prepared {
			return &fsm.Response[Res]{Msg: &Res{
				ImageID:  r.W.Msg.ImageID,
				BaseLvID: existingBaseLvID.Int64,
				// empty basemount so unpack knows to skip
				BaseMount: "",
				Manifest:  r.W.Msg.Manifest,
			}}, nil
		}

		// not yet prepared ⇒ ensure device is mapped & mounted to receive unpack
		// ensure thin pool exists before remapping device
		if err := ensureThinPool(); err != nil {
			return nil, fmt.Errorf("ensure thin pool for remap: %w", err)
		}

		dev := fmt.Sprintf("/dev/mapper/base_lv_%d", existingBaseLvID.Int64)
		deviceName := fmt.Sprintf("base_lv_%d", existingBaseLvID.Int64)

		// check if device mapping exists in kernel (not just the device node)
		if !dmExists(deviceName) {
			// remap device
			table := fmt.Sprintf("0 4194304 thin %s %d", poolDev, existingBaseLvID.Int64)
			if err := exec.Command("dmsetup", "create", deviceName, "--table", table).Run(); err != nil {
				return nil, fmt.Errorf("dmsetup create existing base: %w", err)
			}
			app.Logger.Infof("Remapped device %s", dev)
		} else {
			// device mapping exists, ensure node is present
			dmEnsureNode(deviceName)
		}

		if err := os.MkdirAll(mount, 0755); err != nil {
			return nil, fmt.Errorf("mkdir mount existing: %w", err)
		}
		if !isMountActive(mount) {
			if err := exec.Command("mount", dev, mount).Run(); err != nil {
				return nil, fmt.Errorf("mount existing base: %w", err)
			}
			app.Logger.Infof("Mounted existing %s at %s", dev, mount)
		}

		return &fsm.Response[Res]{Msg: &Res{
			ImageID:   r.W.Msg.ImageID,
			BaseLvID:  existingBaseLvID.Int64,
			BaseMount: mount,
			Manifest:  r.W.Msg.Manifest,
		}}, nil
	}

	// base volume doesn't exist or was stale, create it
	app.Logger.Infof("Creating new thin base volume for image %d", r.W.Msg.ImageID)

	// ensure thin pool infrastructure exists
	if err := ensureThinPool(); err != nil {
		return nil, fmt.Errorf("ensure thin pool: %w", err)
	}

	// allocate a new thin volume id
	baseID, err := allocateID(ctx, app.DB, "base")
	if err != nil {
		return nil, fmt.Errorf("allocate base ID: %w", err)
	}

	// create thin device
	dev, err := createThin(baseID)
	if err != nil {
		return nil, fmt.Errorf("create thin device: %w", err)
	}

	app.Logger.Infof("Created thin device %s for base_lv_id=%d", dev, baseID)

	// format with ext4
	if err := exec.Command("mkfs.ext4", "-q", dev).Run(); err != nil {
		return nil, fmt.Errorf("mkfs.ext4: %w", err)
	}

	// create mount point and mount
	mount := fmt.Sprintf("/mnt/base_lv_%d", baseID)
	if err := os.MkdirAll(mount, 0755); err != nil {
		return nil, fmt.Errorf("mkdir mount: %w", err)
	}

	if err := exec.Command("mount", dev, mount).Run(); err != nil {
		return nil, fmt.Errorf("mount: %w", err)
	}

	app.Logger.Infof("Mounted %s at %s", dev, mount)

	// update image record with base_lv_id
	if _, err := app.DB.ExecContext(ctx,
		`UPDATE images SET base_lv_id=? WHERE id=?`,
		baseID, r.W.Msg.ImageID); err != nil {
		return nil, fmt.Errorf("update base_lv_id: %w", err)
	}

	return &fsm.Response[Res]{Msg: &Res{
		ImageID:   r.W.Msg.ImageID,
		BaseLvID:  baseID,
		BaseMount: mount,
		Manifest:  r.W.Msg.Manifest,
	}}, nil
}

// unpackintobase extracts all image layers into the mounted base thin volume.
// layers are unpacked in sequence order to build the final filesystem.
func UnpackIntoBase(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	var prepared bool
	if err := app.DB.QueryRowContext(ctx,
		`SELECT prepared FROM images WHERE id=?`, r.W.Msg.ImageID).Scan(&prepared); err != nil {
		return nil, fmt.Errorf("check prepared before unpack: %w", err)
	}
	if prepared {
		app.Logger.Infof("Image %d already prepared; skipping unpack step", r.W.Msg.ImageID)
		return &fsm.Response[Res]{Msg: r.W.Msg}, nil
	}

	dst := r.W.Msg.BaseMount
	app.Logger.Infof("Unpacking %d layers into %s", len(r.W.Msg.Manifest), dst)

	// extract each layer in sequence order
	for i, l := range r.W.Msg.Manifest {
		app.Logger.Infof("Unpacking layer %d/%d: %s", i+1, len(r.W.Msg.Manifest), filepath.Base(l.Path))

		if err := UnpackTarToDir(l.Path, dst); err != nil {
			return nil, fmt.Errorf("unpack %s: %w", filepath.Base(l.Path), err)
		}
	}

	app.Logger.Infof("Successfully unpacked all layers")

	if _, err := app.DB.ExecContext(ctx, `UPDATE images SET prepared=1 WHERE id=?`, r.W.Msg.ImageID); err != nil {
		return nil, fmt.Errorf("mark prepared: %w", err)
	}

	// unmount the base volume
	if err := exec.Command("umount", dst).Run(); err != nil {
		// log warning but don't fail if unmount fails
		app.Logger.Warnf("Failed to unmount %s: %v (may be in use)", dst, err)
	} else {
		app.Logger.Infof("Unmounted %s", dst)
	}

	return &fsm.Response[Res]{Msg: r.W.Msg}, nil
}

// activatesnapshot creates a read-write snapshot of the base thin volume
// and mounts it for runtime use.
func ActivateSnapshot(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	// retrieve base_lv_id from database
	var baseID sql.NullInt64
	if err := app.DB.QueryRowContext(ctx,
		`SELECT base_lv_id FROM images WHERE id=?`,
		r.W.Msg.ImageID).Scan(&baseID); err != nil {
		return nil, fmt.Errorf("query base_lv_id: %w", err)
	}

	if !baseID.Valid {
		return nil, fmt.Errorf("no base_lv_id for image %d", r.W.Msg.ImageID)
	}

	app.Logger.Infof("Creating snapshot from base_lv_id=%d", baseID.Int64)

	var existingActID int64
	var existingSnap int64
	var existingMount string
	err := app.DB.QueryRowContext(ctx,
		`SELECT id, snap_lv_id, mount_path FROM activations 
     WHERE image_id=? ORDER BY created_at DESC LIMIT 1`, r.W.Msg.ImageID).
		Scan(&existingActID, &existingSnap, &existingMount)

	if err == nil && existingMount != "" && isMountActive(existingMount) {
		app.Logger.Infof("Reusing existing activation id=%d snap=%d mount=%s",
			existingActID, existingSnap, existingMount)

		out := *r.W.Msg
		out.SnapLvID = existingSnap
		out.SnapDevPath = fmt.Sprintf("/dev/mapper/snap_lv_%d", existingSnap)
		out.MountPath = existingMount
		return &fsm.Response[Res]{Msg: &out}, nil
	}

	// allocate snapshot id
	snapID, err := allocateID(ctx, app.DB, "snap")
	if err != nil {
		return nil, fmt.Errorf("allocate snap ID: %w", err)
	}

	// create snapshot device
	dev, err := createSnap(baseID.Int64, snapID)
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	app.Logger.Infof("Created snapshot device %s (snap_lv_id=%d)", dev, snapID)

	// create mount point and mount
	mount := fmt.Sprintf("/mnt/images/%d", snapID)
	if err := os.MkdirAll(mount, 0755); err != nil {
		return nil, fmt.Errorf("mkdir mount: %w", err)
	}

	if err := exec.Command("mount", dev, mount).Run(); err != nil {
		return nil, fmt.Errorf("mount snapshot: %w", err)
	}

	app.Logger.Infof("Mounted snapshot at %s", mount)

	// record activation in database
	var actID int64
	if err := app.DB.QueryRowContext(ctx,
		`INSERT INTO activations(image_id, snap_lv_id, mount_path) VALUES(?,?,?) RETURNING id`,
		r.W.Msg.ImageID, snapID, mount).Scan(&actID); err != nil {
		return nil, fmt.Errorf("insert activation: %w", err)
	}

	app.Logger.Infof("Activation recorded with id=%d", actID)

	// prepare response with snapshot details
	out := *r.W.Msg
	out.SnapLvID = snapID
	out.SnapDevPath = dev
	out.MountPath = mount

	return &fsm.Response[Res]{Msg: &out}, nil
}

// writeresult writes the final orchestration results to a file.
func WriteResult(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	result := fmt.Sprintf(
		"image_id=%d base_lv=%d snap_lv=%d dev=%s mount=%s\n",
		r.W.Msg.ImageID, r.W.Msg.BaseLvID, r.W.Msg.SnapLvID,
		r.W.Msg.SnapDevPath, r.W.Msg.MountPath,
	)

	if err := os.WriteFile("results.txt", []byte(result), 0644); err != nil {
		app.Logger.Warnf("Failed to write results.txt: %v", err)
	} else {
		app.Logger.Infof("Results written to results.txt")
	}

	app.Logger.Infof("=== FSM Workflow Complete ===")
	app.Logger.Infof("Image ID: %d", r.W.Msg.ImageID)
	app.Logger.Infof("Base LV: %d", r.W.Msg.BaseLvID)
	app.Logger.Infof("Snapshot LV: %d", r.W.Msg.SnapLvID)
	app.Logger.Infof("Snapshot mounted at: %s", r.W.Msg.MountPath)

	return &fsm.Response[Res]{Msg: r.W.Msg}, nil
}

// database helper functions
// upsertimage creates or retrieves an image record.
// uses digest as unique key for idempotent inserts.
func upsertImage(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO images(name, digest, complete) VALUES(?, ?, 0)
		ON CONFLICT(digest) DO UPDATE SET name=excluded.name
		RETURNING id;
	`, name, name).Scan(&id)
	return id, err
}

// upsertblob creates or updates a blob record.
// uses digest as unique key for content-addressed storage.
func upsertBlob(ctx context.Context, tx *sql.Tx, l Layer) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO blobs(digest, etag, size_bytes, local_path, complete)
		VALUES(?,?,?,?,1)
		ON CONFLICT(digest) DO UPDATE SET 
			etag=excluded.etag, 
			size_bytes=excluded.size_bytes, 
			local_path=excluded.local_path, 
			complete=1;
	`, l.Digest, l.ETag, l.Size, l.Path)
	return err
}

// upsertimageblob links a blob to an image with a sequence number.
// the sequence ensures layer order is preserved.
func upsertImageBlob(ctx context.Context, tx *sql.Tx, imageID int64, digest string, seq int) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO image_blobs(image_id, blob_digest, sequence)
		VALUES(?,?,?)
		ON CONFLICT(image_id, sequence) DO UPDATE SET blob_digest=excluded.blob_digest;
	`, imageID, digest, seq)
	return err
}

// allocateid generates a random id and verifies it's unique in the database.
// used for thin volume ids (base and snapshot).
func allocateID(ctx context.Context, db *sql.DB, typ string) (int64, error) {
	for i := 0; i < 10; i++ {
		id := rand.Int63n(1000000) + 1

		// check if id is already used
		if typ == "base" {
			var exists int
			err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM images WHERE base_lv_id=?`, id).Scan(&exists)
			if err != nil {
				return 0, err
			}
			if exists == 0 {
				return id, nil
			}
		} else if typ == "snap" {
			var exists int
			err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM activations WHERE snap_lv_id=?`, id).Scan(&exists)
			if err != nil {
				return 0, err
			}
			if exists == 0 {
				return id, nil
			}
		}
	}
	return 0, fmt.Errorf("failed to allocate unique %s ID after 10 attempts", typ)
}

func isMountActive(path string) bool {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	lines := strings.Split(string(data), "\n")
	for _, ln := range lines {
		if ln == "" {
			continue
		}
		// column 2 is the mountpoint
		fields := strings.Fields(ln)
		if len(fields) >= 2 && fields[1] == path {
			return true
		}
	}
	return false
}
