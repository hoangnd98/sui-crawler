package orchestrator

import (
	"testing"

	"sui-crawler/internal/models"
)

func TestResolveStartCheckpointStartsAfterLastCompletedCheckpoint(t *testing.T) {
	job := &models.CrawlerJob{
		FromCheckpoint: 151002000,
		LastCheckpoint: 151002000,
		EndCheckpoint:  151002500,
	}

	got := resolveStartCheckpoint(job)
	want := int64(151002001)
	if got != want {
		t.Fatalf("resolveStartCheckpoint() = %d, want %d", got, want)
	}
}

func TestResolveStartCheckpointClampsToFromCheckpointWhenNoProgress(t *testing.T) {
	job := &models.CrawlerJob{
		FromCheckpoint: 151002000,
		LastCheckpoint: 0,
		EndCheckpoint:  151002500,
	}

	got := resolveStartCheckpoint(job)
	want := int64(151002000)
	if got != want {
		t.Fatalf("resolveStartCheckpoint() = %d, want %d", got, want)
	}
}
