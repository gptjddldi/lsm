package regression

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinearRegression_TrainAndPredict(t *testing.T) {
	tests := []struct {
		name     string
		x        []uint64
		y        []uint64
		testX    uint64
		expected uint64
		delta    uint64
	}{
		{
			name:     "Simple linear relationship",
			x:        []uint64{1, 2, 3, 4, 5},
			y:        []uint64{2, 4, 6, 8, 10},
			testX:    6,
			expected: 12,
			delta:    1,
		},
		{
			name:     "Non-zero intercept",
			x:        []uint64{1, 2, 3, 4, 5},
			y:        []uint64{3, 5, 7, 9, 11},
			testX:    6,
			expected: 13,
			delta:    1,
		},
		{
			name:     "Larger numbers",
			x:        []uint64{112, 223, 334, 445, 556},
			y:        []uint64{1000, 2000, 3000, 4000, 5000},
			testX:    667,
			expected: 6000,
			delta:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := NewRegression()
			lr.Train(tt.x, tt.y)

			predicted := lr.Predict(tt.testX)
			assert.InDelta(t, tt.expected, predicted, float64(tt.delta), "Prediction should be within delta of expected value")
		})
	}
}
