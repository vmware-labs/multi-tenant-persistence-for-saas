package errors_test

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	. "github.com/vmware-labs/multi-tenant-persistence-for-saas/pkg/errors"
)

func TestErrorToStringConversion_ContextValuesPlacing(t *testing.T) {
	assert := assert.New(t)
	t.Parallel()

	err4 := errors.New("Resource not found")
	err3 := ErrBaseDb.With("Error #3").WithValue("key3", "value3").Wrap(err4)
	err2 := ErrBaseDb.With("Error #2").WithValue("key2.1", "value2.1").
		WithValue("key2.2", "value2.2").Wrap(err3)
	err1 := ErrBaseDb.With("Error #1").Wrap(err2)

	var actualErrStr string = err1.Error()
	matched, err := regexp.MatchString("\\] *\\[", actualErrStr)
	assert.NoError(err)
	assert.False(matched, "Expected error's string version not to place all context values at the very end")

	const expectedErrStr = `Error #1
	Error #2 [key2.1=value2.1, key2.2=value2.2]
		Error #3 [key3=value3]
			Resource not found`
	assert.Equal(expectedErrStr, actualErrStr)
}

func TestErrorToStringConversion_MultilineWithIndentation(t *testing.T) {
	assert := assert.New(t)
	t.Parallel()

	var err error = ErrBaseDb.With("Error #6")
	const numEmbeddedErrors = 5
	for i := numEmbeddedErrors; i > 0; i-- {
		err = ErrBaseDb.With(fmt.Sprintf("Error #%d", i)).Wrap(err)
	}

	actualErrStr := err.Error()
	assert.Equalf(numEmbeddedErrors, strings.Count(actualErrStr, "\n"), "Expected error to contain %d newlines", numEmbeddedErrors)

	expectedNumTabs := 5
	for i := 0; i < numEmbeddedErrors; i++ {
		expectedNumTabs += i
	}
	assert.Equalf(expectedNumTabs, strings.Count(actualErrStr, "\t"), "Expected error to contain %d tabs", expectedNumTabs)

	const expectedErrStr = `Error #1
	Error #2
		Error #3
			Error #4
				Error #5
					Error #6`
	assert.Equal(expectedErrStr, actualErrStr)
}
