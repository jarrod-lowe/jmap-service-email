// Package main implements a placeholder Lambda for the JMAP email plugin.
// This will be removed when real email Lambdas are implemented.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jarrod-lowe/jmap-service-core/pkg/plugincontract"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

func handler(ctx context.Context, request plugincontract.PluginInvocationRequest) (plugincontract.PluginInvocationResponse, error) {
	tracer := otel.Tracer("jmap-email-placeholder")
	ctx, span := tracer.Start(ctx, "PlaceholderHandler")
	defer span.End()

	span.SetAttributes(
		attribute.String("function", "placeholder"),
		attribute.String("request_id", request.RequestID),
		attribute.String("account_id", request.AccountID),
		attribute.String("method", request.Method),
		attribute.String("client_id", request.ClientID),
	)

	logger.InfoContext(ctx, "Received plugin invocation",
		slog.String("request_id", request.RequestID),
		slog.String("account_id", request.AccountID),
		slog.String("method", request.Method),
		slog.String("client_id", request.ClientID),
	)

	// Log the args for debugging
	argsJSON, _ := json.Marshal(request.Args)
	logger.InfoContext(ctx, "Request args",
		slog.String("request_id", request.RequestID),
		slog.String("args", string(argsJSON)),
	)

	// Return serverFail error - placeholder not yet implemented
	response := plugincontract.PluginInvocationResponse{
		MethodResponse: plugincontract.MethodResponse{
			Name: "error",
			Args: map[string]any{
				"type":        "serverFail",
				"description": "Email plugin not yet implemented",
			},
			ClientID: request.ClientID,
		},
	}

	logger.InfoContext(ctx, "Returning placeholder error response",
		slog.String("request_id", request.RequestID),
		slog.String("error_type", "serverFail"),
	)

	return response, nil
}

func main() {
	ctx := context.Background()

	tp, err := xrayconfig.NewTracerProvider(ctx)
	if err != nil {
		logger.Error("FATAL: Failed to initialize tracer provider",
			slog.String("error", err.Error()),
		)
		panic(err)
	}
	otel.SetTracerProvider(tp)

	lambda.Start(otellambda.InstrumentHandler(handler, xrayconfig.WithRecommendedOptions(tp)...))
}
