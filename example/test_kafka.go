package main

import (
	"context"
	"fmt"

	goofys "github.com/kahing/goofys/api"
	"github.com/kahing/goofys/api/common"
)

func main() {
	//fsb := &kafka.KafkaGoofys{}
	config := common.FlagStorage{
		MountPoint: "/tmp/s3",
		DirMode:    0777,
		FileMode:   0666,
		Backend:    &common.KafkaConfig{},
	}

	_, mp, err := goofys.Mount(context.Background(), "jm_log_fs", &config)
	if err != nil {
		panic(fmt.Sprintf("Unable to mount %v: %v", config.MountPoint, err))
	} else {
		mp.Join(context.Background())
	}
}
