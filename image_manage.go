package main 

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/oklog/ulid/v2"
    "github.com/superfly/fsm" 
    fsmv1 "github.com/superfly/fsm/gen/fsm/v1" 
)

type ImageState string

const (
    ImageStateNew        ImageState = "New"
    ImageStateRetrieving ImageState = "Retrieving"
    ImageStateRetrieved  ImageState = "Retrieved"
    ImageStateUnpacking  ImageState = "Unpacking"
    ImageStateUnpacked   ImageState = "Unpacked"
    ImageStateActivating ImageState = "Activating"
    ImageStateActive     ImageState = "Active"
    ImageStateError      ImageState = "Error"
)

type ImageEvent string

const (
    ImageEventDownloadRequested  ImageEvent = "DownloadRequested"
    ImageEventDownloadComplete   ImageEvent = "DownloadComplete"
    ImageEventDownloadFailed     ImageEvent = "DownloadFailed"
    ImageEventUnpackRequested    ImageEvent = "UnpackRequested"
    ImageEventUnpackComplete     ImageEvent = "UnpackComplete"
    ImageEventUnpackFailed       ImageEvent = "UnpackFailed"
    ImageEventActivateRequested  ImageEvent = "ActivateRequested"
    ImageEventActivateComplete   ImageEvent = "ActivateComplete"
    ImageEventActivateFailed     ImageEvent = "ActivateFailed"
    ImageEventRetry              ImageEvent = "Retry" 
)

type ImageFSMData struct {
    ImageID      string 
    TarballPath  string 
    ThinVolumeID int    
    Error        string 
}



// onDownloadRequested initiating an image download.
func onDownloadRequested(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Starting download for image %s...\n", r.ID, data.ImageID)    
    data.TarballPath = fmt.Sprintf("/tmp/downloaded_image_%s.tar.gz", data.ImageID)
    log.Printf("[%s] Image %s download started, simulating completion soon.\n", r.ID, data.ImageID)

    
    time.Sleep(10 * ime.Millisecond) 
}

    return ImageEventDownloadComplete, nil
}

// onDownloadComplete confirms the download is done.
func onDownloadComplete(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Download complete for image %s. Tarball at: %s\n", r.ID, data.ImageID, data.TarballPath)
    return ImageEventUnpackRequested, nil
}

// onDownloadFailed logs the download failure.
func onDownloadFailed(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Download failed for image %s: %s\n", r.ID, data.ImageID, data.Error)
    return fsm.Noop, nil 
}

// onUnpackRequested initiating image unpacking.
func onUnpackRequested(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Starting unpack for image %s from %s...\n", r.ID, data.ImageID, data.TarballPath)
    
    // thin ID from image_manager.go function
    data.ThinVolumeID = 123 + len(data.ImageID) 

}

    time.Sleep(20 * ime.Millisecond) 

    log.Printf("[%s] Image %s unpack started, simulating completion soon.\n", r.ID, data.ImageID)
    return ImageEventUnpackComplete, nil
}

// onUnpackComplete confirms unpacking is done.
func onUnpackComplete(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Unpack complete for image %s. Thin Volume ID: %d\n", r.ID, data.ImageID, data.ThinVolumeID)
    return ImageEventActivateRequested, nil
}

// onUnpackFailed logs unpack failure.
func onUnpackFailed(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Unpack failed for image %s: %s\n", r.ID, data.ImageID, data.Error)
    return fsm.Noop, nil // Stay in Error state
}

// onActivateRequested simulates activating the image (snapshot creation).
func onActivateRequested(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Activating image %s (creating snapshot from Thin Volume %d)...\n", r.ID, data.ImageID, data.ThinVolumeID)
    time.Sleep(5 * ime.Millisecond) 
    log.Printf("[%s] Image %s activation started, simulating completion soon.\n", r.ID, data.ImageID)
    return ImageEventActivateComplete, nil
}

// onActivateComplete confirms activation.
func onActivateComplete(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Image %s successfully activated!\n", r.ID, data.ImageID)
    return fsm.Noop, nil 
}

// onActivateFailed logs activation failure.
func onActivateFailed(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] Activation failed for image %s: %s\n", r.ID, data.ImageID, data.Error)
    return fsm.Noop, nil // Stay in Error state
}

// onError is a generic handler for when the FSM enters an error state.
func onError(ctx context.Context, r *fsm.Run, data *ImageFSMData) (fsm.Event, error) {
    log.Printf("[%s] FSM entered ERROR state for image %s. Last error: %s\n", r.ID, data.ImageID, data.Error)
    return fsm.Noop, nil
}

// registerImageFSM defines and registers the image processing FSM.
func registerImageFSM(manager *fsm.Manager) error {
    builder := fsm.NewBuilder[ImageFSMData](
        "image-processor", 
        ImageStateNew.String(), 
        fsm.WithDescription("Manages the lifecycle of container images"),
        fsm.WithConcurrency(1), 
    )

    // Define states and transitions
    builder.State(ImageStateNew.String()).
        Transition(ImageEventDownloadRequested.String(), ImageStateRetrieving.String(), onDownloadRequested)

    builder.State(ImageStateRetrieving.String()).
        Transition(ImageEventDownloadComplete.String(), ImageStateRetrieved.String(), onDownloadComplete).
        Transition(ImageEventDownloadFailed.String(), ImageStateError.String(), onDownloadFailed)

    builder.State(ImageStateRetrieved.String()).
        Transition(ImageEventUnpackRequested.String(), ImageStateUnpacking.String(), onUnpackRequested)

    builder.State(ImageStateUnpacking.String()).
        Transition(ImageEventUnpackComplete.String(), ImageStateUnpacked.String(), onUnpackComplete).
        Transition(ImageEventUnpackFailed.String(), ImageStateError.String(), onUnpackFailed)

    builder.State(ImageStateUnpacked.String()).
        Transition(ImageEventActivateRequested.String(), ImageStateActivating.String(), onActivateRequested)

    builder.State(ImageStateActivating.String()).
        Transition(ImageEventActivateComplete.String(), ImageStateActive.String(), onActivateComplete).
        Transition(ImageEventActivateFailed.String(), ImageStateError.String(), onActivateFailed)

    builder.State(ImageStateActive.String()).
        WithDescription("Image is fully processed and ready for use.")

    builder.State(ImageStateError.String()).
        WithDescription("An error occurred during image processing.").
        Transition(ImageEventRetry.String(), ImageStateNew.String(), nil) // Allow retrying from error

    // Build and register the FSM
    if err := manager.Register(context.Background(), builder.FSM()); err != nil {
        return fmt.Errorf("failed to register image-processor FSM: %w", err)
    }
    return nil
}

/*
func main() {
    // Configure the FSM Manager
    cfg := fsm.Config{
        Logger: logrus.New(), 
        DBPath: "./fsm_db",   
        Queues: map[string]int{
            "image-queue": 5,
        },
    }

    manager, err := fsm.New(cfg)
    if err != nil {
        log.Fatalf("Failed to create FSM manager: %v", err)
    }
    defer manager.Shutdown( * ime.Second) 
    if err := registerImageFSM(manager); err != nil {
        log.Fatalf("Failed to register image FSM: %v", err)
    }

    log.Println("Image FSM registered. Starting example runs...")

    // Start an image processing FSM run
    imageID1 := "image-alpine-latest"
    run1, err := manager.Start(context.Background(), "image-processor", "process", imageID1, ImageFSMData{ImageID: imageID1})
    if err != nil {
        log.Printf("Failed to start FSM for %s: %v", imageID1, err)
    } else {
        log.Printf("Started FSM for %s, Run ID: %s", imageID1, run1.ID.String())
    }

    imageID2 := "image-ubuntu-20.04"
    run2, err := manager.Start(context.Background(), "image-processor", "process", imageID2, ImageFSMData{ImageID: imageID2})
    if err != nil {
        log.Printf("Failed to start FSM for %s: %v", imageID2, err)
    } else {
        log.Printf("Started FSM for %s, Run ID: %s", imageID2, run2.ID.String())
    }

    // Wait for FSMs to complete
    if run1 != nil {
        log.Printf("Waiting for FSM %s to complete...", run1.ID.String())
        if err := manager.Wait(context.Background(), run1.ID); err != nil {
            log.Printf("FSM %s completed with error: %v", run1.ID.String(), err)
        } else {
            log.Printf("FSM %s completed successfully.", run1.ID.String())
        }
    }

    if run2 != nil {
        log.Printf("Waiting for FSM %s to complete...", run2.ID.String())
        if err := manager.Wait(context.Background(), run2.ID); err != nil {
            log.Printf("FSM %s completed with error: %v", run2.ID.String(), err)
        } else {
            log.Printf("FSM %s completed successfully.", run2.ID.String())
        }
    }

    log.Println("All example FSM runs finished. Shutting down.")
}
*/
