package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type openAPIDocument struct {
	OpenAPI    string                 `json:"openapi"`
	Info       openAPIInfo            `json:"info"`
	Servers    []openAPIServer        `json:"servers,omitempty"`
	Paths      map[string]openAPIPath `json:"paths"`
	Components openAPIComponents      `json:"components"`
}

type openAPIInfo struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type openAPIServer struct {
	URL string `json:"url"`
}

type openAPIPath map[string]openAPIOperation

type openAPIOperation struct {
	Summary     string                     `json:"summary,omitempty"`
	Description string                     `json:"description,omitempty"`
	Tags        []string                   `json:"tags,omitempty"`
	Parameters  []openAPIParameter         `json:"parameters,omitempty"`
	RequestBody *openAPIRequestBody        `json:"requestBody,omitempty"`
	Responses   map[string]openAPIResponse `json:"responses"`
}

type openAPIParameter struct {
	Name        string         `json:"name"`
	In          string         `json:"in"`
	Description string         `json:"description,omitempty"`
	Required    bool           `json:"required,omitempty"`
	Schema      map[string]any `json:"schema"`
}

type openAPIRequestBody struct {
	Required bool                        `json:"required"`
	Content  map[string]openAPIMediaType `json:"content"`
}

type openAPIMediaType struct {
	Schema map[string]any `json:"schema"`
}

type openAPIResponse struct {
	Description string                      `json:"description"`
	Content     map[string]openAPIMediaType `json:"content,omitempty"`
}

type openAPIComponents struct {
	Schemas map[string]map[string]any `json:"schemas"`
}

func RegisterSwaggerRoutes(router *gin.Engine) {
	router.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/")
	})
	router.GET("/swagger/", serveSwaggerUI)
	router.GET("/swagger/doc.json", serveOpenAPIDocument)
}

func serveOpenAPIDocument(c *gin.Context) {
	c.JSON(http.StatusOK, buildOpenAPIDocument())
}

func serveSwaggerUI(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(http.StatusOK, `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SUI Crawler API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: '/swagger/doc.json',
      dom_id: '#swagger-ui'
    });
  </script>
</body>
</html>`)
}

func buildOpenAPIDocument() openAPIDocument {
	jsonRef := func(name string) map[string]any {
		return map[string]any{"$ref": "#/components/schemas/" + name}
	}

	return openAPIDocument{
		OpenAPI: "3.0.3",
		Info: openAPIInfo{
			Title:       "SUI Crawler API",
			Version:     "1.0.0",
			Description: "API for creating and monitoring crawler jobs.",
		},
		Servers: []openAPIServer{{URL: "/"}},
		Paths: map[string]openAPIPath{
			"/health": {
				"get": {
					Summary: "Health check",
					Tags:    []string{"system"},
					Responses: map[string]openAPIResponse{
						"200": {
							Description: "Service is healthy",
						},
					},
				},
			},
			"/api/v1/jobs": {
				"post": {
					Summary:     "Create a crawler job",
					Description: "Create a crawler job with explicit checkpoint boundaries.",
					Tags:        []string{"jobs"},
					RequestBody: &openAPIRequestBody{
						Required: true,
						Content: map[string]openAPIMediaType{
							"application/json": {Schema: jsonRef("CreateJobRequest")},
						},
					},
					Responses: map[string]openAPIResponse{
						"201": {
							Description: "Crawler job created",
							Content: map[string]openAPIMediaType{
								"application/json": {Schema: jsonRef("JobResponse")},
							},
						},
						"400": {
							Description: "Invalid request",
							Content: map[string]openAPIMediaType{
								"application/json": {Schema: jsonRef("ErrorResponse")},
							},
						},
					},
				},
				"get": {
					Summary: "List crawler jobs",
					Tags:    []string{"jobs"},
					Parameters: []openAPIParameter{
						{
							Name:        "status",
							In:          "query",
							Description: "Filter by pending, progressing, or completed",
							Schema:      map[string]any{"type": "string"},
						},
					},
					Responses: map[string]openAPIResponse{
						"200": {
							Description: "Crawler jobs",
							Content: map[string]openAPIMediaType{
								"application/json": {
									Schema: map[string]any{
										"type":  "array",
										"items": jsonRef("JobResponse"),
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/jobs/{id}": {
				"get": {
					Summary: "Get crawler job",
					Tags:    []string{"jobs"},
					Parameters: []openAPIParameter{
						{
							Name:        "id",
							In:          "path",
							Description: "MongoDB job id",
							Required:    true,
							Schema:      map[string]any{"type": "string"},
						},
					},
					Responses: map[string]openAPIResponse{
						"200": {
							Description: "Crawler job",
							Content: map[string]openAPIMediaType{
								"application/json": {Schema: jsonRef("JobResponse")},
							},
						},
						"404": {
							Description: "Job not found",
							Content: map[string]openAPIMediaType{
								"application/json": {Schema: jsonRef("ErrorResponse")},
							},
						},
					},
				},
			},
			"/api/v1/jobs/{id}/retry": {
				"post": {
					Summary: "Retry crawler job",
					Tags:    []string{"jobs"},
					Parameters: []openAPIParameter{
						{
							Name:        "id",
							In:          "path",
							Description: "MongoDB job id",
							Required:    true,
							Schema:      map[string]any{"type": "string"},
						},
					},
					Responses: map[string]openAPIResponse{
						"200": {
							Description: "Crawler job reset to pending",
							Content: map[string]openAPIMediaType{
								"application/json": {Schema: jsonRef("JobResponse")},
							},
						},
					},
				},
			},
		},
		Components: openAPIComponents{
			Schemas: map[string]map[string]any{
				"CreateJobRequest": {
					"type": "object",
					"required": []string{
						"fromCheckpoint",
						"endCheckpoint",
					},
					"properties": map[string]any{
						"fromCheckpoint": map[string]any{
							"type":    "integer",
							"format":  "int64",
							"example": 0,
						},
						"endCheckpoint": map[string]any{
							"type":    "integer",
							"format":  "int64",
							"example": 1000,
						},
					},
				},
				"JobResponse": {
					"type": "object",
					"properties": map[string]any{
						"id":             map[string]any{"type": "string", "example": "682a1b2c3d4e5f6a7b8c9d0e"},
						"fromCheckpoint": map[string]any{"type": "integer", "format": "int64", "example": 0},
						"lastCheckpoint": map[string]any{"type": "integer", "format": "int64", "example": 499},
						"endCheckpoint":  map[string]any{"type": "integer", "format": "int64", "example": 1000},
						"status":         map[string]any{"type": "string", "example": "progressing"},
						"error":          map[string]any{"type": "string", "example": ""},
						"created_at":     map[string]any{"type": "string", "format": "date-time"},
						"updated_at":     map[string]any{"type": "string", "format": "date-time"},
						"completed_at":   map[string]any{"type": "string", "format": "date-time"},
					},
				},
				"ErrorResponse": {
					"type": "object",
					"properties": map[string]any{
						"error": map[string]any{"type": "string", "example": "invalid request body"},
					},
				},
			},
		},
	}
}
