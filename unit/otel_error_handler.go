package unit

import (
	"context"

	"go.gearno.de/kit/log"
)

type (
	otelErrorHandler struct {
		logger *log.Logger
		ctx    context.Context
	}
)

func (h *otelErrorHandler) Handle(err error) {
	if err != nil {
		h.logger.ErrorCtx(h.ctx, "open telemetry error", log.Error(err))
	}
}
