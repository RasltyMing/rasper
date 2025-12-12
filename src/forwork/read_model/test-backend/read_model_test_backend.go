package main

import "github.com/gin-gonic/gin"

func main() {
	r := gin.Default()
	r.GET("/*path", func(c *gin.Context) {
		c.JSON(200, gin.H{"url": c.Request.URL.String()})
	})
	r.Run(":8080")
}
