package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	goofys "github.com/kahing/goofys/api"
	"github.com/kahing/goofys/api/common"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	wg := new(sync.WaitGroup)
	bgCtx := context.Background()
	cCtx, ctxCancel := context.WithCancel(bgCtx)
	config := common.FlagStorage{
		MountPoint: "/tmp/s3",
		DirMode:    0777,
		FileMode:   0666,
		Backend: &common.ForwarderBackendConfig{
			WG:             wg,
			CTX:            cCtx,
			PersistentFile: "/tmp/jm-log-fs.meta",
		},
	}
	go http.ListenAndServe(":1771", nil)
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		log.Printf("receive stop signal(%s), exiting jm-log-fs...\n", sig)
		ctxCancel()
		log.Println("context canceled")
		goofys.TryUnmount(config.MountPoint)
	}()
	_, mp, err := goofys.Mount(cCtx, "jm-log-fs", &config)
	if err != nil {
		panic(fmt.Sprintf("Unable to mount %v: %v", config.MountPoint, err))
	} else {
		mp.Join(cCtx)
		log.Println("goofys stopped.")
	}
	wg.Wait()
}
