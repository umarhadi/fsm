package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/superfly/fsm"
)

func main() {
	// parse cli flags
	golangFlag := flag.Bool("golang", false, "Use golang image family")
	nodeFlag := flag.Bool("node", false, "Use node image family")
	pythonFlag := flag.Bool("python", false, "Use python image family")
	flag.Parse()

	// determine image family (default: golang)
	imageFamily := "golang"
	if *nodeFlag {
		imageFamily = "node"
	} else if *pythonFlag {
		imageFamily = "python"
	} else if *golangFlag {
		imageFamily = "golang"
	}

	// setup graceful shutdown on interrupt signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// initialize logger
	logger := stdLogger()
	logger.Info("FSM orchestrator starting...")
	logger.Infof("Image family selected: %s", imageFamily)

	// initialize sqlite database for image/blob tracking
	db, err := InitDB()
	if err != nil {
		logger.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	logger.Info("Database initialized: fsm.db")

	// try to become the manager with short backoff
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

	// initialize s3 client (anonymous access to public bucket)
	s3c, err := NewS3Client(ctx, "flyio-platform-hiring-challenge", "us-east-1")
	if err != nil {
		logger.Fatalf("Failed to initialize S3 client: %v", err)
	}
	logger.Info("S3 client initialized")

	// create application context with all dependencies
	app := &App{DB: db, S3: s3c, Logger: logger}

	// build fsm workflow: fetchlistandblobs → preparethinbase → unpackintobase → activatesnapshot → writeresult → done
	builder := fsm.
		Register[Req, Res](mgr, "run").
		Start("FetchListAndBlobs", WithApp(app, FetchListAndBlobs)).
		To("PrepareThinBase", WithApp(app, PrepareThinBase)).
		To("UnpackIntoBase", WithApp(app, UnpackIntoBase)).
		To("ActivateSnapshot", WithApp(app, ActivateSnapshot)).
		To("WriteResult", WithApp(app, WriteResult)).
		End("done")

	start, resume, err := builder.Build(ctx)
	if err != nil {
		logger.Fatalf("Failed to build FSM: %v", err)
	}

	// resume any incomplete fsm runs from previous executions
	if err := resume(ctx); err != nil {
		logger.Warnf("Resume error (may be expected if no prior runs): %v", err)
	}

	// start a new fsm run
	runID := RandID()
	logger.Infof("Starting FSM run for image family: %s", imageFamily)

	version, err := start(ctx, runID, fsm.NewRequest(&Req{
		Family: imageFamily,
	}, &Res{}))
	if err != nil {
		logger.Fatalf("Failed to start FSM: %v", err)
	}
	logger.Infof("FSM run started: id=%s version=%s", runID, version)

	// wait for workflow completion
	logger.Info("Waiting for FSM workflow to complete...")
	if err := mgr.WaitByID(ctx, runID); err != nil {
		logger.Errorf("FSM run failed: %v", err)
		os.Exit(1)
	}

	logger.Info("FSM workflow completed successfully")

	// graceful shutdown of fsm manager
	logger.Info("Shutting down FSM manager...")
	mgr.Shutdown(10 * time.Second)

	logger.Info("FSM orchestrator stopped")
}
