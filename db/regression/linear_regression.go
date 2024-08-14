package regression

type LinearRegression struct {
	Slope     float64
	Intercept float64
}

func NewRegression() *LinearRegression {
	return &LinearRegression{}
}

func (lr *LinearRegression) Predict(x uint64) uint64 {
	return uint64(lr.Slope*float64(x) + lr.Intercept)
}

// Train y = alpha + beta*x
func (lr *LinearRegression) Train(x, y []uint64) {
	if len(x) != len(y) {
		panic("stat: slice length mismatch")
	}

	xu, xv := MeanVariance(x)
	yu := Mean(y)
	cov := covarianceMeans(x, y, xu, yu)

	beta := cov / xv
	alpha := yu - beta*xu

	lr.Slope = beta
	lr.Intercept = alpha
}

func Mean(x []uint64) float64 {
	sum := float64(0)
	for _, xi := range x {
		sum += float64(xi)
	}
	return sum / float64(len(x))
}

func MeanVariance(x []uint64) (mean, variance float64) {
	n := float64(len(x))
	if n == 0 {
		return 0, 0
	}

	mean = Mean(x)
	variance = 0
	for _, xi := range x {
		diff := float64(xi) - mean
		variance += diff * diff
	}
	variance /= n

	return mean, variance
}

func covarianceMeans(x, y []uint64, xmean, ymean float64) float64 {
	n := float64(len(x))
	if n == 0 {
		return 0
	}

	var cov float64
	for i := range x {
		cov += (float64(x[i]) - xmean) * (float64(y[i]) - ymean)
	}
	return cov / n
}

//func stringToInt(s string) (uint64, error) {
//	if len(s) == 0 {
//		return 0, nil
//	}
//
//	if len(s) > 6 {
//		return 0, fmt.Errorf("string should be less than 5 characters")
//	}
//
//	base := uint64(36) // 10 (digits) + 26 (lowercase)
//	charToValue := make(map[rune]uint64)
//
//	// 숫자 (0-9) 매핑
//	for i := 0; i < 10; i++ {
//		charToValue[rune('0'+i)] = uint64(i)
//	}
//
//	// 소문자 (a-z) 매핑
//	for i := 0; i < 26; i++ {
//		charToValue[rune('a'+i)] = uint64(i + 10)
//	}
//
//	var result uint64 = 0
//	for _, char := range s {
//		value, exists := charToValue[char]
//		if !exists {
//			return 0, fmt.Errorf("invalid character is contained")
//		}
//		result = result*base + value
//	}
//
//	return result, nil
//}
