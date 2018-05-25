package metrics

import (
	"util/log"
)

type Output interface {
	Output(output interface{})
}

type DefaultOutput struct {

}

func (do *DefaultOutput) Output(output interface{}) {
	if apiMetric, ok := output.(*OpenFalconCustomData); ok {
		log.Debug("metric %v", apiMetric)
	} else if tps, ok := output.(*TpsStats); ok {
		log.Debug("metric %v", tps)
	} else {
		log.Debug("metric %v", output)
	}
}
