# fsm

container image orchestrator using [superfly/fsm](https://github.com/superfly/fsm) for durable state management. fetches OCI images from s3, unpacks them into devicemapper thin volumes, and creates read-write snapshots.

## how it works

```
FetchListAndBlobs → PrepareThinBase → UnpackIntoBase → ActivateSnapshot → WriteResult → done
```

every step is persisted to boltdb, so if it crashes mid-way it picks up where it left off.

## build & run

```bash
go build -o fsm .

# needs root for devicemapper
sudo ./fsm              # default: golang
sudo ./fsm --node
sudo ./fsm --python
```

running the same family twice reuses existing base volumes and skips downloads.

## cleanup

```bash
sudo ./cleanup.sh
```

removes all dm devices, mounts, loop devices, backing files, and databases.

## requirements

- go 1.25+
- linux with devicemapper support
- root access
- `dmsetup`, `losetup`, `fallocate`, `mkfs.ext4`
