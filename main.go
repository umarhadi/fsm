package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/superfly/fsm"
)

func main() {
	golangFlag := flag.Bool("golang", false, "Use golang image family")
	nodeFlag := flag.Bool("node", false, "Use node image family")
	pythonFlag := flag.Bool("python", false, "Use python image family")
	flag.Parse()

	imageFamily := "golang"
	if *nodeFlag {
		imageFamily = "node"
	} else if *pythonFlag {
		imageFamily = "python"
	} else if *golangFlag {
		imageFamily = "golang"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := stdLogger()
	logger.Info("FSM orchestrator starting...")
	logger.Infof("Image family selected: %s", imageFamily)

	db, err := InitDB()
	if err != nil {
		logger.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	logger.Info("Database initialized: fsm.db")

	var mgr *fsm.Manager
	{
		backoffs := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
		var err error
		for i, d := range backoffs {
			mgr, err = fsm.New(fsm.Config{
				Logger: logger,
				DBPath: "./fsmdb",
				Queues: map[string]int{"q": 4},
			})
			if err == nil {
				break
			}
			if i == len(backoffs)-1 {
				logger.Warnf("Another instance likely owns ./fsmdb (init error: %v). Exiting gracefully.", err)
				return
			}
			logger.Warnf("FSM init failed (%v). Retrying in %s...", err, d)
			time.Sleep(d)
		}
	}
	logger.Info("FSM manager initialized: ./fsmdb")

	s3c, err := NewS3Client(ctx, "flyio-platform-hiring-challenge", "us-east-1")
	if err != nil {
		logger.Fatalf("Failed to initialize S3 client: %v", err)
	}
	logger.Info("S3 client initialized")

	app := &App{DB: db, S3: s3c, Logger: logger}

	// phase 1: prepare — deterministic run ID, runs once per family
	prepareBuilder := fsm.
		Register[Req, Res](mgr, "prepare").
		Start("FetchManifest", WithApp(app, FetchManifest)).
		To("DownloadBlobs", WithApp(app, DownloadBlobs)).
		To("PrepareThinBase", WithApp(app, PrepareThinBase)).
		To("UnpackIntoBase", WithApp(app, UnpackIntoBase)).
		End("done")

	startPrepare, resumePrepare, err := prepareBuilder.Build(ctx)
	if err != nil {
		logger.Fatalf("Failed to build prepare FSM: %v", err)
	}

	// phase 2: activate — fresh run ID each invocation
	activateBuilder := fsm.
		Register[Req, Res](mgr, "activate").
		Start("ActivateSnapshot", WithApp(app, ActivateSnapshot)).
		End("done")

	startActivate, resumeActivate, err := activateBuilder.Build(ctx)
	if err != nil {
		logger.Fatalf("Failed to build activate FSM: %v", err)
	}

	// resume any in-progress runs from prior crashes
	if err := resumePrepare(ctx); err != nil {
		logger.Warnf("Resume prepare error: %v", err)
	}
	if err := resumeActivate(ctx); err != nil {
		logger.Warnf("Resume activate error: %v", err)
	}

	// check if prepare phase already done
	var prepared bool
	_ = db.QueryRowContext(ctx,
		`SELECT prepared FROM images WHERE name=?`, imageFamily).Scan(&prepared)

	if !prepared {
		logger.Infof("Running prepare phase for %s", imageFamily)
		prepareID := "prepare-" + imageFamily
		if _, err := startPrepare(ctx, prepareID, fsm.NewRequest(&Req{Family: imageFamily}, &Res{})); err != nil {
			logger.Fatalf("Failed to start prepare FSM: %v", err)
		}
		if err := mgr.WaitByID(ctx, prepareID); err != nil {
			logger.Errorf("Prepare FSM failed: %v", err)
			os.Exit(1)
		}
		// mark prepared externally — not inside an FSM step
		if _, err := db.ExecContext(ctx,
			`UPDATE images SET prepared=1 WHERE name=?`, imageFamily); err != nil {
			logger.Warnf("Failed to mark prepared: %v", err)
		}
		logger.Infof("Prepare phase complete for %s", imageFamily)
	}

	// retrieve image_id and base_lv_id from DB for the activate step
	var imageID, baseLvID int64
	if err := db.QueryRowContext(ctx,
		`SELECT id, base_lv_id FROM images WHERE name=? AND prepared=1`,
		imageFamily).Scan(&imageID, &baseLvID); err != nil {
		logger.Fatalf("Failed to retrieve prepared image: %v", err)
	}

	// run activate phase (always — creates a new snapshot each invocation)
	activateID := RandID()
	logger.Infof("Running activate phase (id=%s)", activateID)

	if _, err := startActivate(ctx, activateID, fsm.NewRequest(
		&Req{Family: imageFamily},
		&Res{ImageID: imageID, BaseLvID: baseLvID},
	)); err != nil {
		logger.Fatalf("Failed to start activate FSM: %v", err)
	}
	if err := mgr.WaitByID(ctx, activateID); err != nil {
		logger.Errorf("Activate FSM failed: %v", err)
		os.Exit(1)
	}

	// write results — not a crash-worthy operation
	var snapLvID int64
	var mountPath string
	_ = db.QueryRowContext(ctx,
		`SELECT snap_lv_id, mount_path FROM activations WHERE image_id=? ORDER BY created_at DESC LIMIT 1`,
		imageID).Scan(&snapLvID, &mountPath)

	result := fmt.Sprintf("image_id=%d base_lv=%d snap_lv=%d mount=%s\n",
		imageID, baseLvID, snapLvID, mountPath)
	_ = os.WriteFile("results.txt", []byte(result), 0644)
	logger.Infof("done: %s", strings.TrimSpace(result))

	// graceful shutdown
	mgr.Shutdown(10 * time.Second)
	logger.Info("FSM orchestrator stopped")
}
