package main

import (
    "database/sql"
    "fmt"
    "log"
    "time"

    _ "github.com/mattn/go-sqlite3" // Import for SQLite driver
)

const (
    dbPath = "./flyd_images.db" // Path to the SQLite database file
)

// Image represents an image entry in our database
type Image struct {
    ID             int
    S3Key          string
    Status         string
    ThinVolumeName sql.NullString // Use sql.NullString for nullable fields
    SnapshotName   sql.NullString
    CreatedAt      time.Time
    UpdatedAt      time.Time
}

// DBManager manages interactions with the SQLite database
type DBManager struct {
    db *sql.DB
}

// NewDBManager creates a new DBManager instance and initializes the database
func NewDBManager() (*DBManager, error) {
    db, err := InitDB()
    if err != nil {
        return nil, fmt.Errorf("failed to initialize database: %w", err)
    }
    return &DBManager{db: db}, nil
}

// InitDB initializes the SQLite database and creates the 'images' table if it doesn't exist.
func InitDB() (*sql.DB, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open database: %w", err)
    }

    // Create 'images' table
    createTableSQL := `
    CREATE TABLE IF NOT EXISTS images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        s3_key TEXT UNIQUE NOT NULL,
        status TEXT NOT NULL,
        thin_volume_name TEXT,
        snapshot_name TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    `
    _, err = db.Exec(createTableSQL)
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create images table: %w", err)
    }

    log.Printf("SQLite database initialized at %s", dbPath)
    return db, nil
}

// Close closes the database connection.
func (dm *DBManager) Close() error {
    return dm.db.Close()
}

// ImageExists checks if an image with the given s3_key exists in the database.
func (dm *DBManager) ImageExists(s3Key string) (bool, error) {
    query := "SELECT EXISTS(SELECT 1 FROM images WHERE s3_key = ?);"
    var exists bool
    err := dm.db.QueryRow(query, s3Key).Scan(&exists)
    if err != nil {
        return false, fmt.Errorf("failed to check if image exists for s3_key '%s': %w", s3Key, err)
    }
    return exists, nil
}

// GetImageState retrieves the current status of an image by its s3_key.
func (dm *DBManager) GetImageState(s3Key string) (string, error) {
    query := "SELECT status FROM images WHERE s3_key = ?;"
    var status string
    err := dm.db.QueryRow(query, s3Key).Scan(&status)
    if err == sql.ErrNoRows {
        return "", fmt.Errorf("image with s3_key '%s' not found", s3Key)
    } else if err != nil {
        return "", fmt.Errorf("failed to get image state for s3_key '%s': %w", s3Key, err)
    }
    return status, nil
}

// GetImage retrieves a full Image record by its s3_key.
func (dm *DBManager) GetImage(s3Key string) (*Image, error) {
    query := "SELECT id, s3_key, status, thin_volume_name, snapshot_name, created_at, updated_at FROM images WHERE s3_key = ?;"
    row := dm.db.QueryRow(query, s3Key)

    var img Image
    err := row.Scan(&img.ID, &img.S3Key, &img.Status, &img.ThinVolumeName, &img.SnapshotName, &img.CreatedAt, &img.UpdatedAt)
    if err == sql.ErrNoRows {
        return nil, fmt.Errorf("image with s3_key '%s' not found", s3Key)
    } else if err != nil {
        return nil, fmt.Errorf("failed to retrieve image for s3_key '%s': %w", s3Key, err)
    }
    return &img, nil
}

// AddImage adds a new image entry to the database.
func (dm *DBManager) AddImage(s3Key string, initialStatus string) error {
    insertSQL := `
    INSERT INTO images (s3_key, status)
    VALUES (?, ?);
    `
    _, err := dm.db.Exec(insertSQL, s3Key, initialStatus)
    if err != nil {
        // Check for unique constraint violation
        if err.Error() == "UNIQUE constraint failed: images.s3_key" {
            return fmt.Errorf("image with s3_key '%s' already exists", s3Key)
        }
        return fmt.Errorf("failed to add image '%s': %w", s3Key, err)
    }
    log.Printf("Added new image '%s' with status '%s' to database.", s3Key, initialStatus)
    return nil
}

// UpdateImageState updates the status, thin_volume_name, and snapshot_name for an image.
// It uses s3_key to identify the image.
func (dm *DBManager) UpdateImageState(s3Key string, status string, thinVolumeName, snapshotName sql.NullString) error {
    updateSQL := `
    UPDATE images
    SET status = ?, thin_volume_name = ?, snapshot_name = ?, updated_at = CURRENT_TIMESTAMP
    WHERE s3_key = ?;
    `
    result, err := dm.db.Exec(updateSQL, status, thinVolumeName, snapshotName, s3Key)
    if err != nil {
        return fmt.Errorf("failed to update image '%s': %w", s3Key, err)
    }

    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("failed to get rows affected for update of '%s': %w", s3Key, err)
    }
    if rowsAffected == 0 {
        return fmt.Errorf("no image found with s3_key '%s' to update", s3Key)
    }

    log.Printf("Updated image '%s' to status '%s'. Thin Volume: %s, Snapshot: %s.", s3Key, status, thinVolumeName.String, snapshotName.String)
    return nil
}

// Example usage (for testing purposes, not part of the main library)
func main() {
    dm, err := NewDBManager()
    if err != nil {
        log.Fatalf("Error initializing DB Manager: %v", err)
    }
    defer dm.Close()

    testS3Key1 := "flyio-platform-hiring-challenge/images/image_abcd123.tar.gz"
    testS3Key2 := "flyio-platform-hiring-challenge/images/image_efgh456.tar.gz"

    // Test 1: Add a new image
    log.Println("\n--- Test 1: Adding new image ---")
    err = dm.AddImage(testS3Key1, "New")
    if err != nil {
        log.Printf("Error adding image: %v", err)
    }

    // Test 2: Check if image exists
    log.Println("\n--- Test 2: Checking if image exists ---")
    exists, err := dm.ImageExists(testS3Key1)
    if err != nil {
        log.Printf("Error checking image existence: %v", err)
    } else {
        log.Printf("Image '%s' exists: %t", testS3Key1, exists)
    }

    // Test 3: Get image state
    log.Println("\n--- Test 3: Getting image state ---")
    status, err := dm.GetImageState(testS3Key1)
    if err != nil {
        log.Printf("Error getting image state: %v", err)
    } else {
        log.Printf("Status of '%s': %s", testS3Key1, status)
    }

    // Test 4: Update image state
    log.Println("\n--- Test 4: Updating image state ---")
    thinVolName := "flyd-vol-abcd123"
    snapshotName := "flyd-snap-abcd123"
    err = dm.UpdateImageState(testS3Key1, "Active", sql.NullString{String: thinVolName, Valid: true}, sql.NullString{String: snapshotName, Valid: true})
    if err != nil {
        log.Printf("Error updating image state: %v", err)
    }

    // Test 5: Verify updated state
    log.Println("\n--- Test 5: Verifying updated state ---")
    updatedImage, err := dm.GetImage(testS3Key1)
    if err != nil {
        log.Printf("Error getting updated image: %v", err)
    } else {
        log.Printf("Updated Image: ID: %d, S3Key: %s, Status: %s, ThinVolume: %s, Snapshot: %s, CreatedAt: %s, UpdatedAt: %s",
            updatedImage.ID, updatedImage.S3Key, updatedImage.Status, updatedImage.ThinVolumeName.String, updatedImage.SnapshotName.String, updatedImage.CreatedAt, updatedImage.UpdatedAt)
    }

    // Test 6: Attempt to add an existing image (should return error)
    log.Println("\n--- Test 6: Attempting to add existing image ---")
    err = dm.AddImage(testS3Key1, "New")
    if err != nil {
        log.Printf("Expected error adding existing image: %v", err)
    }

    // Test 7: Add another new image (for subsequent FSM testing)
    log.Println("\n--- Test 7: Adding another new image ---")
    err = dm.AddImage(testS3Key2, "New")
    if err != nil {
        log.Printf("Error adding second image: %v", err)
    }
}

