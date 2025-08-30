package errors

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// APIError represents an error response according to interface contracts
type APIError struct {
	Error APIErrorDetails `json:"error"`
}

// APIErrorDetails contains error information
type APIErrorDetails struct {
	Code    string                   `json:"code"`
	Message string                   `json:"message"`
	Details []map[string]interface{} `json:"details,omitempty"`
}

// NewAPIError creates a new API error
func NewAPIError(code, message string, details map[string]interface{}) *APIError {
	var detailsList []map[string]interface{}
	if details != nil {
		detailsList = []map[string]interface{}{details}
	}

	return &APIError{
		Error: APIErrorDetails{
			Code:    code,
			Message: message,
			Details: detailsList,
		},
	}
}

// NewValidationError creates a validation error with field details
func NewValidationError(message string, field, issue string) *APIError {
	return &APIError{
		Error: APIErrorDetails{
			Code:    "INVALID_ARGUMENT",
			Message: message,
			Details: []map[string]interface{}{
				{
					"field": field,
					"issue": issue,
				},
			},
		},
	}
}

// GRPCStatusToHTTPStatus converts gRPC status codes to HTTP status codes
// according to interface contracts
func GRPCStatusToHTTPStatus(grpcCode codes.Code) int {
	switch grpcCode {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Aborted:
		return http.StatusConflict
	case codes.Internal:
		return http.StatusInternalServerError
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusRequestTimeout
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

// GRPCErrorToAPIError converts a gRPC error to an API error
func GRPCErrorToAPIError(err error) (int, *APIError) {
	if err == nil {
		return http.StatusOK, nil
	}

	// Extract gRPC status
	grpcStatus := status.Convert(err)
	httpStatus := GRPCStatusToHTTPStatus(grpcStatus.Code())

	// Convert gRPC code to string
	var code string
	switch grpcStatus.Code() {
	case codes.InvalidArgument:
		code = "INVALID_ARGUMENT"
	case codes.Unauthenticated:
		code = "UNAUTHENTICATED"
	case codes.PermissionDenied:
		code = "PERMISSION_DENIED"
	case codes.NotFound:
		code = "NOT_FOUND"
	case codes.AlreadyExists:
		code = "ALREADY_EXISTS"
	case codes.FailedPrecondition:
		code = "FAILED_PRECONDITION"
	case codes.Aborted:
		code = "ABORTED"
	case codes.Internal:
		code = "INTERNAL"
	case codes.Unavailable:
		code = "UNAVAILABLE"
	case codes.DeadlineExceeded:
		code = "DEADLINE_EXCEEDED"
	case codes.ResourceExhausted:
		code = "RESOURCE_EXHAUSTED"
	default:
		code = "INTERNAL"
	}

	return httpStatus, NewAPIError(code, grpcStatus.Message(), nil)
}
