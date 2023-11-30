// api.go
package main

import (
	"html/template"
	"net/http"
	"github.com/gin-gonic/gin"
)

// SetupAPI initializes the API routes.
func SetupAPI(lsmTree MemTable) *gin.Engine {
	r := gin.Default()

	// Load HTML template from file
	htmlTemplate, err := template.ParseFiles("index.html")
	if err != nil {
		panic("Error loading HTML template: " + err.Error())
	}

	// API routes
	r.GET("/get/:key", func(c *gin.Context) {
		key := c.Param("key")
		value, exists := lsmTree.Get(key)
		c.JSON(http.StatusOK, gin.H{"key": key, "value": value, "exists": exists})
	})

	r.POST("/set", func(c *gin.Context) {
		var json KeyValue
		if err := c.BindJSON(&json); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		lsmTree.Set(json.Key, json.Value)
		print(json.Key,json.Value)
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})

	r.DELETE("/delete/:key", func(c *gin.Context) {
		key := c.Param("key")
		lsmTree.Delete(key)
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})

	r.GET("/html", func(c *gin.Context) {
		data := struct {
			MemTable map[string]string
		}{
			MemTable: lsmTree.GetData(),
		}
		htmlTemplate.Execute(c.Writer, data)
	})

	return r
}
