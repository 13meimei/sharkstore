package alarm2

import (
	"time"

	"util/log"
	"errors"
)

func (s *Server) timingDbPulling() {
	duration := s.conf.MysqlPullingDurationSec
	t := time.NewTimer(duration)
	for {
		select {
		case <-t.C:
			s.tableAppPulling()
			s.tableGlobalRulePulling()
			s.tableClusterRulePulling()
			s.tableReceiverPulling()

			t.Reset(duration)
		}
	}
}

func (s *Server) tableAppPulling() {
	ret, err := s.getTableAppData()
	if err != nil {
		log.Error("pull table app failed: %v", err)
	}

	if err := s.mapAppUpdate(ret); err != nil {
		log.Error("update map app failed: %v", err)
	}
}

func (s *Server) tableGlobalRulePulling() {

}

func (s *Server) tableClusterRulePulling() {

}

func (s *Server) tableReceiverPulling() {

}

func (s *Server) mapAppUpdate(data []TableApp) error {
	if len(data) == 0 {
		return errors.New("no data in table app")
	}

	newMapApp := make[]
	for _, d := range data {


	}
	return nil
}