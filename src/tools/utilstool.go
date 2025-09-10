package tools

import (
	uuid "github.com/satori/go.uuid"
)

func NewUuid() string {
	id := uuid.NewV4()
	return id.String()
}
