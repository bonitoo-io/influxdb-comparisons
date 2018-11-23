package influxdb

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevopsSystem produces Influx-specific queries for the devops single-host case over a 12hr period.
type InfluxDevopsSystem struct {
	InfluxDevops
}

func NewInfluxQLDevopsSystem(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxDevops)
	return &InfluxDevopsSystem{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevopsSystem(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxDevops)
	return &InfluxDevopsSystem{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsSystem) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	interval := d.AllInterval.RandWindow(d.queryInterval)
	nn := rand.Perm(d.ScaleVar)[:1]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("value%05d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		if d.language == InfluxQL {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("tag9 = '%s'", s))
		} else {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf(`r.hostname == "%s"`, s))
		}
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT  moving_average(count(\"service_up\"),5)/moving_average(count(\"service_under_maintenance\"),5) AS \"service_time\" FROM status WHERE %s and  time >= '%s' and time < '%s' group by time(10m) fill(null)", combinedHostnameClause, interval.StartString(), interval.EndString())
		//query = fmt.Sprintf("SELECT  moving_average(count(\"v0\"),5)/moving_average(count(\"v1\"),5) AS \"service_time\" FROM m0 WHERE %s and  time >= '%s' and time < '%s' group by time(10m) fill(null)", combinedHostnameClause, interval.StartString(), interval.EndString())
	} else { // Flux
		query = fmt.Sprintf(`from(db:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "cpu" and r._field == "usage_user" and (%s)) `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) `+
			`|> window(period:1m) `+
			`|> max() `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			combinedHostnameClause)
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Maintance frequency, rand host, %s by 10m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
