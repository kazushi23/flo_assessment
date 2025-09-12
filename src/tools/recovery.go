package tools

import (
	"flo/assessment/config/log"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Recovery struct {
}

func Recover(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			// Log the panic with stack trace
			zapFields := []zap.Field{
				zap.Any("panic", r),
				log.Any("stack", string(debug.Stack())),
			}
			log.Logger.Error("Recovered from panic", zapFields...)

			// Return 500 to client
			c.AbortWithStatus(500)
		}
	}()
	c.Next()
}
