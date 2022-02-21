package main

import (
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pyroscope-io/client/pyroscope"

	"github.com/prabhatsharma/zinc/pkg/routes"
	"github.com/prabhatsharma/zinc/pkg/zutils"
)

func main() {
	r := gin.New()
	// Recovery middleware recovers from any panics and writes a 500 if there was one.
	r.Use(gin.Recovery())

	routes.SetPrometheus(r) // Set up Prometheus.
	routes.SetRoutes(r)     // Set up all API routes.

	// pyroscope server
	serverAddress := os.Getenv("PYROSCOPE_SERVER_ADDRESS")
	if serverAddress == "" {
		serverAddress = "http://localhost:4040"
	}
	pyroscope.Start(pyroscope.Config{
		ApplicationName: "zinc.app",
		ServerAddress:   serverAddress,
		Logger:          pyroscope.StandardLogger,
	})

	// Run the server
	PORT := zutils.GetEnv("PORT", "4080")
	r.Run(":" + PORT)
}
