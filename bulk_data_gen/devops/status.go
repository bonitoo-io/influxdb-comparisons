package devops

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var (
	StatusByteString = []byte("status") // heap optimization
	// Field keys for 'air condition indoor' points.
	ServiceUpFieldKey          = []byte("service_up")
	ServiceUnderMaintenanceKey = []byte("service_under_maintenance")
)

type StatusMeasurement struct {
	timestamp                   time.Time
	serviceUp                   Distribution
	serviceUnderMaintenance     Distribution
	sendServiceUnderMaintenance bool
}

func NewStatusMeasurement(start time.Time) *StatusMeasurement {
	//state
	serviceUp := TSD(0, 1, 0)
	serviceUnderMaintenance := TSD(0, 1, 0)

	return &StatusMeasurement{
		timestamp:               start,
		serviceUp:               serviceUp,
		serviceUnderMaintenance: serviceUnderMaintenance,
	}
}

func (m *StatusMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	m.sendServiceUnderMaintenance = rand.Intn(10) > 7
	m.serviceUp.Advance()
	m.serviceUnderMaintenance.Advance()
}

func (m *StatusMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(StatusByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendField(ServiceUpFieldKey, int(m.serviceUp.Get()))
	if m.sendServiceUnderMaintenance {
		p.AppendField(ServiceUnderMaintenanceKey, int(m.serviceUnderMaintenance.Get()))
	}
	return true
}
