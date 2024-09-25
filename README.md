# promsketch

This repository provides PromSketch package for Prometheus and VictoriaMetrics.


### Install Go
```
wget https://go.dev/dl/go1.22.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

### Run EHUniv test
```
go test -v -timeout 0 -run ^TestExpoHistogramUnivMonOptimizedCAIDA$ github.com/froot/promsketch
```

### Run EHKLL test
```
go test -v -timeout 0 -run ^TestCostAnalysisQuantile$ github.com/froot/promsketch
```

### Integration with Prometheus

```
git clone git@github.com:zzylol/prometheus.git
cd prometheus
go mod tidy
make build
```

### Integration with VictoriaMetrics

```
git clone git@github.com:zzylol/VictoriaMetrics.git
cd VictoriaMetrics
go mod vendor
```
Compile:
```
cd VictoriaMetrics
make victoria-metrics
make vmalert
```