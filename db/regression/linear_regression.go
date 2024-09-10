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
