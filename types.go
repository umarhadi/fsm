package main

import (
	"context"
	"database/sql"

	"github.com/sirupsen/logrus"
	"github.com/superfly/fsm"
)

// app holds shared dependencies for fsm steps.
type App struct {
	DB     *sql.DB        // sqlite database for image/blob tracking
	S3     *S3Client      // s3 client for downloading blobs
	Logger *logrus.Logger // structured logger
}

// req is the fsm request payload.
type Req struct {
	Family string // image family to fetch (e.g., "golang", "node", "python")
}

// res is the fsm response payload, passed between states.
type Res struct {
	ImageID     int64   // database id of the image
	BaseLvID    int64   // thin volume id for base layer
	SnapLvID    int64   // thin volume id for snapshot
	BaseMount   string  // mount path for base volume
	SnapDevPath string  // device path for snapshot (/dev/mapper/snap_lv_{id})
	MountPath   string  // mount path for snapshot (/mnt/images/{id})
	Manifest    []Layer // ordered list of image layers
}

// layer represents a single image layer from s3.
type Layer struct {
	Key    string // s3 object key
	ETag   string // s3 etag for deduplication
	Size   int64  // size in bytes
	Digest string // sha256 content digest
	Path   string // local filesystem path after download
}

// step is a function signature for fsm transition functions.
type Step func(context.Context, *fsm.Request[Req, Res], *App) (*fsm.Response[Res], error)

// withapp is a closure that injects the app dependency into fsm steps.
func WithApp(app *App, fn Step) func(context.Context, *fsm.Request[Req, Res]) (*fsm.Response[Res], error) {
	return func(ctx context.Context, r *fsm.Request[Req, Res]) (*fsm.Response[Res], error) {
		return fn(ctx, r, app)
	}
}
