package api

import (
	"github.com/gin-gonic/gin"

	"sui-crawler/internal/client"
	"sui-crawler/internal/repository"
)

// NewRouter creates and configures the Gin router with all routes and middleware.
func NewRouter(repo *repository.JobRepository, suiClient *client.SuiClient) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.Logger())
	RegisterSwaggerRoutes(router)

	handler := NewHandler(repo, suiClient)

	// Health check
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	v1 := router.Group("/api/v1")
	{
		v1.POST("/jobs", handler.CreateJob)
		v1.GET("/jobs", handler.ListJobs)
		v1.GET("/jobs/:id", handler.GetJob)
		v1.POST("/jobs/:id/retry", handler.RetryJob)
	}

	return router
}
