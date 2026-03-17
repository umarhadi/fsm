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

// fetchmanifest lists all layers for the image family from s3 and upserts the image record.
// no downloads happen here — that's DownloadBlobs's job.
func FetchManifest(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	layers, err := app.S3.ListFamily(ctx, r.Msg.Family)
	if err != nil {
		return nil, fmt.Errorf("list S3 family: %w", err)
	}
	if len(layers) == 0 {
		return nil, fsm.Abort(fmt.Errorf("no layers found for family %s", r.Msg.Family))
	}

	tx, err := app.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	imageID, err := upsertImage(ctx, tx, r.Msg.Family)
	if err != nil {
		return nil, fmt.Errorf("upsert image: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	app.Logger.Infof("Found %d layers for %s (image_id=%d)", len(layers), r.Msg.Family, imageID)
	return &fsm.Response[Res]{Msg: &Res{ImageID: imageID, Manifest: layers}}, nil
}

// downloadblobs downloads all layers for the image family from s3.
// skips blobs that are already present locally (idempotent).
func DownloadBlobs(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	ok, _ := AcquireLock(ctx, app.DB, "fetch:"+r.Msg.Family, "pid", 15*time.Second)
	if !ok {
		return nil, fmt.Errorf("lock contention on fetch:%s", r.Msg.Family)
	}
	defer ReleaseLock(context.Background(), app.DB, "fetch:"+r.Msg.Family, "pid")

	tx, err := app.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var manifest []Layer
	for i, l := range r.W.Msg.Manifest {
		path, dig, err := app.S3.DownloadIfMissing(ctx, &l)
		if err != nil {
			return nil, fmt.Errorf("download blob %s: %w", l.Key, err)
		}
		l.Path, l.Digest = path, dig

		if err := upsertBlob(ctx, tx, l); err != nil {
			return nil, fmt.Errorf("upsert blob %s: %w", l.Digest, err)
		}
		if err := upsertImageBlob(ctx, tx, r.W.Msg.ImageID, l.Digest, i); err != nil {
			return nil, fmt.Errorf("upsert image_blob: %w", err)
		}
		manifest = append(manifest, l)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE images SET complete=1 WHERE id=?`, r.W.Msg.ImageID); err != nil {
		return nil, fmt.Errorf("mark image complete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	app.Logger.Infof("Downloaded %d blobs", len(manifest))
	out := *r.W.Msg
	out.Manifest = manifest
	return &fsm.Response[Res]{Msg: &out}, nil
}

// preparethinbase creates a thin pool and base volume for the image.
// phase 1 only runs when prepared=false, so base_lv_id is always null here.
func PrepareThinBase(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	ok, _ := AcquireLock(ctx, app.DB, "thin:base", "pid", 15*time.Second)
	if !ok {
		return nil, fmt.Errorf("lock contention thin:base")
	}
	defer ReleaseLock(context.Background(), app.DB, "thin:base", "pid")

	if err := ensureThinPool(); err != nil {
		return nil, fmt.Errorf("ensure thin pool: %w", err)
	}

	baseID, err := allocateID(ctx, app.DB, "base")
	if err != nil {
		return nil, fmt.Errorf("allocate base ID: %w", err)
	}

	dev, err := createThin(baseID)
	if err != nil {
		return nil, fmt.Errorf("create thin device: %w", err)
	}

	if err := exec.Command("mkfs.ext4", "-q", dev).Run(); err != nil {
		return nil, fmt.Errorf("mkfs.ext4: %w", err)
	}

	mount := fmt.Sprintf("/mnt/base_lv_%d", baseID)
	if err := os.MkdirAll(mount, 0755); err != nil {
		return nil, fmt.Errorf("mkdir mount: %w", err)
	}
	if err := exec.Command("mount", dev, mount).Run(); err != nil {
		return nil, fmt.Errorf("mount: %w", err)
	}

	if _, err := app.DB.ExecContext(ctx,
		`UPDATE images SET base_lv_id=? WHERE id=?`, baseID, r.W.Msg.ImageID); err != nil {
		return nil, fmt.Errorf("update base_lv_id: %w", err)
	}

	app.Logger.Infof("Created and mounted base_lv_%d at %s", baseID, mount)
	out := *r.W.Msg
	out.BaseLvID = baseID
	out.BaseMount = mount
	return &fsm.Response[Res]{Msg: &out}, nil
}

// unpackintobase extracts all image layers into the mounted base volume.
// phase 1 only runs when prepared=false so this always has real work to do.
func UnpackIntoBase(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	dst := r.W.Msg.BaseMount
	app.Logger.Infof("Unpacking %d layers into %s", len(r.W.Msg.Manifest), dst)

	for i, l := range r.W.Msg.Manifest {
		app.Logger.Infof("Unpacking layer %d/%d: %s", i+1, len(r.W.Msg.Manifest), filepath.Base(l.Path))
		if err := UnpackTarToDir(l.Path, dst); err != nil {
			return nil, fmt.Errorf("unpack %s: %w", filepath.Base(l.Path), err)
		}
	}

	app.Logger.Infof("Successfully unpacked all layers")

	if err := exec.Command("umount", dst).Run(); err != nil {
		app.Logger.Warnf("Failed to unmount %s: %v (may be in use)", dst, err)
	} else {
		app.Logger.Infof("Unmounted %s", dst)
	}

	return &fsm.Response[Res]{Msg: r.W.Msg}, nil
}

// activatesnapshot creates a read-write snapshot from the base volume and mounts it.
// receives base_lv_id via r.W.Msg — no DB re-query needed.
func ActivateSnapshot(ctx context.Context, r *fsm.Request[Req, Res], app *App) (*fsm.Response[Res], error) {
	baseID := r.W.Msg.BaseLvID

	app.Logger.Infof("Creating snapshot from base_lv_id=%d", baseID)

	snapID, err := allocateID(ctx, app.DB, "snap")
	if err != nil {
		return nil, fmt.Errorf("allocate snap ID: %w", err)
	}

	dev, err := createSnap(baseID, snapID)
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	app.Logger.Infof("Created snapshot device %s (snap_lv_id=%d)", dev, snapID)

	mount := fmt.Sprintf("/mnt/images/%d", snapID)
	if err := os.MkdirAll(mount, 0755); err != nil {
		return nil, fmt.Errorf("mkdir mount: %w", err)
	}
	if err := exec.Command("mount", dev, mount).Run(); err != nil {
		return nil, fmt.Errorf("mount snapshot: %w", err)
	}

	app.Logger.Infof("Mounted snapshot at %s", mount)

	var actID int64
	if err := app.DB.QueryRowContext(ctx,
		`INSERT INTO activations(image_id, snap_lv_id, mount_path) VALUES(?,?,?) RETURNING id`,
		r.W.Msg.ImageID, snapID, mount).Scan(&actID); err != nil {
		return nil, fmt.Errorf("insert activation: %w", err)
	}

	app.Logger.Infof("Activation recorded with id=%d", actID)

	out := *r.W.Msg
	out.SnapLvID = snapID
	out.SnapDevPath = dev
	out.MountPath = mount
	return &fsm.Response[Res]{Msg: &out}, nil
}

// database helpers

func upsertImage(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO images(name, digest, complete) VALUES(?, ?, 0)
		ON CONFLICT(digest) DO UPDATE SET name=excluded.name
		RETURNING id;
	`, name, name).Scan(&id)
	return id, err
}

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

func upsertImageBlob(ctx context.Context, tx *sql.Tx, imageID int64, digest string, seq int) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO image_blobs(image_id, blob_digest, sequence)
		VALUES(?,?,?)
		ON CONFLICT(image_id, sequence) DO UPDATE SET blob_digest=excluded.blob_digest;
	`, imageID, digest, seq)
	return err
}

func allocateID(ctx context.Context, db *sql.DB, typ string) (int64, error) {
	for i := 0; i < 10; i++ {
		id := rand.Int63n(1000000) + 1
		if typ == "base" {
			var exists int
			if err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM images WHERE base_lv_id=?`, id).Scan(&exists); err != nil {
				return 0, err
			}
			if exists == 0 {
				return id, nil
			}
		} else if typ == "snap" {
			var exists int
			if err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM activations WHERE snap_lv_id=?`, id).Scan(&exists); err != nil {
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
		fields := strings.Fields(ln)
		if len(fields) >= 2 && fields[1] == path {
			return true
		}
	}
	return false
}
