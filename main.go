package main

import (
    "context"
    "log"
    "time"

    "github.com/sirupsen/logrus"
    "github.com/superfly/fsm" 

    "your_module_path/image_fsm"
)

func main() {
    logger := logrus.New()
    logger.SetLevel(logrus.DebugLevel)


    cfg := fsm.Config{
        Logger: logger,
        DBPath: "./fsm_data",
        Queues: map[string]int{
            "default": 10,
            "image-processing": 2,
        },
    }

    manager, err := fsm.New(cfg)
    if err != nil {
        log.Fatalf("Failed to create FSM Manager: %v", err)
    }


    image_fsm.RegisterImageFSM(manager)

    go func() {
        if err := manager.Run(context.Background()); err != nil {
            log.Fatalf("FSM Manager failed to run: %v", err)
        }
    }()

    // Example of starting an FSM
    imageID := "my-new-image-001"
    initialRequest := image_fsm.ImageRequest{
        ImageID:     imageID,
        S3ObjectKey: "images/my-image.tar.gz",
    }
    initialResponse := image_fsm.ImageResponse{}

    // The manager.Start call initiates the FSM defined by ImageLifecycleAction
    runVersion, err := manager.Start(
        context.Background(),
        imageID, 
        fsm.NewRequest(&initialRequest, &initialResponse),
        fsm.WithQueue("image-processing"),
    )
    if err != nil {
        log.Fatalf("Failed to start Image Lifecycle FSM: %v", err)
    }
    log.Printf("Started Image FSM for ID %s, Run Version: %s", imageID, runVersion.String())

    // wait for the FSM to complete
    log.Printf("Waiting for Image FSM %s to complete...", imageID)
    if err := manager.WaitByID(context.Background(), imageID); err != nil {
        log.Printf("Image FSM %s completed with error: %v", imageID, err)
    } else {
        log.Printf("Image FSM %s completed successfully.", imageID)
    }

    // eventually shutdown the manager ...
    manager.Shutdown( * ime.Second)
    log.Println("Application shutdown.")
}
